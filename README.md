# Commissions Pipeline - Kafka Without a Framework

A working multi-stage commissions pipeline built on Kafka, SQL Server and .NET 10. An agency
uploads a CSV of insurance policies, the pipeline calculates the commission owed on every
row, and it reports back when the whole batch is done.

I built this pipeline to learn Kafka properly. I had worked with Kafka before through
Silverback, which is a good library, but Silverback handles the producer, the consumer, the
offsets and the retries on your behalf. That left me able to describe what Kafka does
without being able to explain how. This version therefore uses the raw `Confluent.Kafka`
client and nothing else. Every consumer loop, every offset commit and every partition
decision is written out in full, because those are the parts I wanted to understand.

Volume is the second reason this project exists. A single agency can send a million rows,
and there are hundreds of agencies. Designing for fifty rows and designing for a million are
different jobs, and the difference shows up in places that are easy to miss until it is too
late.

## What it does

An agency sends a CSV. Each row is one insurance policy with a premium amount and a
commission rate. The commission owed depends on the premium, the rate, and a multiplier that
comes from the broker's tier, which lives in the database.

```
agency CSV file
      |
      v
[1] INGESTOR        reads the file, stages every row in SQL Server, publishes one
      |             message per row that parsed cleanly
      v
  commissions              (6 partitions)
      |
      v
[2] LOOKUP          reads the broker's tier and multiplier from the database, one
      |             row at a time, and adds them to the message
      v
  commissions.enriched
      |
      v
[3] CALCULATOR      premium x rate x multiplier, rounded to the cent
      |
      v
  commissions.calculated
      |
      v
[4] CONSOLIDATOR    writes the result, counts what has been done against what was
      |             expected, marks the batch complete          (not built yet)
      v
[5] NOTIFIER        emails the agent once, and only once, when the batch finishes
                                                                (not built yet)
```

Three of the five workers are built and working today. The consolidator and the notifier
are the next two to write.

## The things worth reading the code for

**The whole file lands in the database before anything is published.** There is a `Batches`
table holding one row per uploaded file, and a `RawRows` table holding every line of that
file exactly as it arrived, including the lines that would not parse. Months later you can
still answer "what did the agency actually send us", which is a question that comes up more
often than you would think.

**Rows that fail to parse are stored, not skipped.** A blank broker id or a premium that is
not a number gets a row in `RawRows` with the original text, a line number and an error
message. The batch records how many rows were accepted and how many were rejected, and the
two together account for every line in the file.

**Reprocessing the same batch cannot double-count anything.** The results table has a
composite primary key of row id plus batch id, so a message that gets delivered twice
overwrites its own row instead of adding a second one. That is what makes it safe to commit
Kafka offsets only after the database write succeeds, and to let a crash replay work rather
than lose it.

**The message shrinks as it moves down the pipeline.** The raw message has six fields, the
enriched one has eight, and the calculated one has five. Once the commission has been worked
out, the premium and the rate and the multiplier are nobody's business any more. The message
carries the answer, not the working.

## A measurement that changed the design

Kafka decides which partition a message goes to by hashing the key you give it. I keyed the
messages on the broker id first, because that seemed natural, and then I looked at where the
messages had actually landed.

```
                     partition:   0    1    2    3    4    5     total
  key = broker id                 0    0    0   19   31    0      50
  key = row id                    7    9    7   11   10    6      50
```

The test file has four brokers in it. Four distinct keys cannot fill six partitions, so two
of them were always going to be empty, and two pairs of brokers hashed to the same partition
anyway, which left four partitions with nothing in them at all.

That matters because a consumer can only read from a partition it owns. With broker id as
the key I could have started six consumers and four of them would have sat there doing
nothing, forever, with no error and no warning. The number of distinct keys sets the ceiling
on parallelism, not the number of partitions.

Switching the key to the row id fixed it. What I gave up is ordering: all of one broker's
rows used to arrive in order on a single partition, and now they are spread out. That is
fine here, because every commission row is calculated on its own and nothing depends on the
row before it. If the business rule had been different, the skew would have been the price
of correctness and I would have had to live with it.

## What you need to run it

- .NET 10 SDK
- Docker Desktop
- About 4GB of free memory, mostly for SQL Server

Everything else runs in containers. There is nothing to install on the host.

## Setting it up

**1. Clone it and create your `.env` file.**

```bash
git clone <this-repo>
cd Kafka
copy .env.example .env
```

Open `.env` and set a password. SQL Server rejects weak ones at startup with an error that
does not tell you that is the problem, so use at least eight characters with upper case,
lower case, a digit and a symbol.

**2. Start the containers.**

```bash
docker compose up -d
docker compose ps
```

Wait until Kafka and SQL Server both report `healthy` rather than just `Up`. SQL Server takes
twenty to forty seconds the first time. If you skip this and start a worker immediately, it
will fail on its first connection.

**3. Create the topics.**

```bash
docker exec kafka-practice kafka-topics --create --topic commissions --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092
docker exec kafka-practice kafka-topics --create --topic commissions.enriched --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092
docker exec kafka-practice kafka-topics --create --topic commissions.calculated --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092
```

Six partitions is a deliberate choice. It divides evenly by one, two, three and six, so you
can run that many consumers without anything sitting idle.

**4. Tell the workers how to reach the database.**

The connection string is not in the repo. Set it with user secrets, using the same password
you put in `.env`:

```bash
dotnet user-secrets set "ConnectionStrings:CommissionsDb" "Server=localhost,1433;Database=CommissionsDb;User Id=sa;Password=YOUR_PASSWORD_HERE;TrustServerCertificate=True;" --project src/Commissions.Ingestor
dotnet user-secrets set "ConnectionStrings:CommissionsDb" "Server=localhost,1433;Database=CommissionsDb;User Id=sa;Password=YOUR_PASSWORD_HERE;TrustServerCertificate=True;" --project src/Commissions.Lookup
```

User secrets are stored in your own user profile, well away from the repository.
`TrustServerCertificate=True` is there because the container uses a self-signed certificate.
It is fine on a laptop and it would be wrong in production, where you would use a managed
identity against Azure SQL and have no password in the string at all.

**5. Create the database.**

```bash
dotnet ef database update --project src/Commissions.Infrastructure --startup-project src/Commissions.Ingestor
```

You need the EF tools for this. If you do not have them, run
`dotnet tool install --global dotnet-ef` first. Both project arguments are required, because
the migrations live in the infrastructure project while the connection string lives in the
ingestor.

This creates the tables and seeds four brokers with their tiers and multipliers.

**6. Point the ingestor at the CSV.**

Open `src/Commissions.Ingestor/appsettings.json` and set `CsvFilePath` to wherever you
cloned the repository:

```json
  "Ingestion": {
    "FileChunkSize": 10000,
    "CsvFilePath": "C:/wherever/you/cloned/Kafka/data/commissions.csv"
  }
```

An absolute path is easiest. A relative one resolves against the worker's own folder rather
than where you typed the command, which trips people up.

**7. Run it.**

```bash
dotnet run --project src/Commissions.Ingestor
```

The ingestor reads the file, stages the rows and publishes them, then exits when it has
finished, because ingestion is a batch job rather than a service.

Then, in two more terminals:

```bash
dotnet run --project src/Commissions.Lookup
dotnet run --project src/Commissions.Calculator
```

The lookup and calculator workers run until you stop them with Ctrl+C. Each one picks up
whatever is waiting in its topic and then waits for more.

## Checking that it worked

How many messages made it to each partition of each topic:

```bash
docker exec kafka-practice kafka-get-offsets --bootstrap-server localhost:9092 --topic commissions.calculated
```

The output is `topic:partition:count`. Older tutorials use
`kafka-run-class kafka.tools.GetOffsetShell` for this, which no longer works on recent
versions because the class was moved.

How far behind a consumer group is:

```bash
docker exec kafka-practice kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group lookup-workers
```

The lag column is the one that matters. It is what you would alert on in production, and on
Azure it is what KEDA uses to decide how many copies of a worker to run.

What actually landed in the database:

```bash
docker exec sql-practice /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "YOUR_PASSWORD_HERE" -No -d CommissionsDb -Q "SELECT BatchId, ExpectedRowCount, RejectedRowCount, Status FROM Batches"
```

## Try this

**Watch two consumers share the work.** Start a second copy of the lookup worker in another
terminal while the first one is running, then run the ingestor again. Kafka splits the six
partitions between them, three each, with no configuration and no code change. Check it with
the consumer group command above. Kill one of them and the other picks up all six within a
second or two.

**Break a few rows.** Add some malformed lines to the end of `data/commissions.csv` and run
the ingestor again. A blank broker id, a premium that reads `notanumber`, a line with only
two fields. They land in `RawRows` with `IsParseable` set to false, the original line
preserved and an error explaining what went wrong, and the batch counts them as rejected
rather than expected.

**Run the same file twice.** Every run gets a new batch id, so the rows do not collide and
the counts stay separate. Without a batch id, a second run of the same file would make the
final tally meaningless.

## Project layout

```
src/Commissions.Contracts       the three message types, and nothing else
src/Commissions.Core            domain logic and interfaces. No Kafka, no EF Core.
src/Commissions.Infrastructure  the adapters - EF Core, the Kafka publisher, the lookup
src/Commissions.Ingestor        stage 1
src/Commissions.Lookup          stage 2
src/Commissions.Calculator      stage 3
tests/Commissions.UnitTests     the calculator and the CSV parser
```

The contracts project has no dependencies at all, on purpose. Everything else references it,
so anything added to it is forced on every service that reads a message. The core project
holds the commission calculation and the CSV parser as pure functions, which is why the unit
tests run in under a second with Docker switched off.

## What this deliberately does not do

**No schema registry.** Messages are JSON, so a change to a producer can quietly break a
consumer. A real system would use Avro or Protobuf with a registry enforcing compatibility.

**No distributed tracing.** There is a batch id on every message and it is enough to follow a
row through the pipeline by hand, but it is not W3C trace context and there is no
OpenTelemetry.

**One broker.** Replication, leader election and in-sync replica behaviour are things I
understand from the model and from reading `--describe` output, not things this setup
exercises.

**No dead letter queue yet.** A row that fails is logged and its offset is committed, which
means it is dropped. The consolidator and the DLQ are the next things to build.

I would rather write these limitations down than leave someone to discover them.

## Running the tests

```bash
dotnet test
```

The tests cover the commission rounding and the CSV parser. Neither test needs Docker or a
database, because the logic under test has no I/O.

Two of those tests exist because of a bug they caught. The parser was setting a "this row
parsed correctly" flag on every failure path and never on the success path, so the parser
silently rejected every valid row. All the tests covering rejection passed. The two tests
covering success are what found the bug.
