# Design

This document describes how the commissions pipeline works and why it is built the way it is.
Each section opens with the user story that section exists to satisfy, so that anyone reading
the code can see what a component is for before working out how it does it.

## When this was written

Stages 1 to 4 were built first, and this document describes them as they actually work, so
parts of it were written after the code rather than before it. Stage 5, the outbox and the date
handling were done the other way round. Those were specified here first and built afterwards,
and the design did not have to change once the code was written.

A design document that reads as though every decision was made before the code is less useful
than one that says where it was not, which is why the order above is stated plainly. Section 10
lists what is still missing.

## Contents

1. [The people this system serves](#1-the-people-this-system-serves)
2. [What the pipeline has to do](#2-what-the-pipeline-has-to-do)
3. [Why there are five stages](#3-why-there-are-five-stages)
4. [One message per row](#4-one-message-per-row)
5. [When the same message arrives twice](#5-when-the-same-message-arrives-twice)
6. [Knowing a batch is finished](#6-knowing-a-batch-is-finished)
7. [Telling the agency once, and only once](#7-telling-the-agency-once-and-only-once)
8. [Dates, and why a row identifier is not enough](#8-dates-and-why-a-row-identifier-is-not-enough)
9. [What happens when things break](#9-what-happens-when-things-break)
10. [What is not built, and why](#10-what-is-not-built-and-why)

---

## 1. The people this system serves

Four people care about this pipeline, and most of the decisions recorded here trace back to one
of them.

**The commissions agency** uploads a file of policies and wants to know what it earned.

**The finance team** closes off the month and has to stand behind the numbers.

**Whoever is on support** gets called when something breaks, usually out of hours.

**The next developer** picks this up and needs to know why it is shaped like this.

---

## 2. What the pipeline has to do

> **As a commissions agency, I want to upload my file of policies and get on with my day, so
> that I am not sitting watching a progress bar while a million rows are processed.**

An agency uploads a CSV file. Every row is one policy, carrying a premium amount and a
commission rate. The pipeline works out the commission owed on each row, saves the result, and
reports back when the whole file is done. The upload itself finishes as soon as the rows are
safely stored, and everything after that happens in the background.

Two things make this harder than it sounds.

The first is volume. One agency can send a million rows, and there are hundreds of agencies. A
design that works for fifty rows can collapse at a million, usually because it does something
once per row that should have been done once for the whole file.

The second is the notification at the end. Everywhere else in this pipeline a failure can be
fixed by doing the work again, because writing the same result twice leaves the same answer
behind. Sending the same email twice leaves the agency with two messages rather than one. Once
an email has gone out there is no way to take it back. Sections 6 and 7 are shaped almost
entirely by that one difference between a database write that can be repeated and an email that
cannot.

---

## 3. Why there are five stages

> **As the next developer on this system, I want to understand why the work is split up the
> way it is, so that I do not merge two stages together and undo the reason they were
> separated.**

```
agency CSV
    |
    v
[1] INGESTOR      saves every row to SQL Server, then publishes one message per row
    |
    v
  commissions
    |
    v
[2] LOOKUP        reads the broker tier and multiplier from the database
    |
    v
  commissions.enriched
    |
    v
[3] CALCULATOR    does the arithmetic, talks to nothing
    |
    v
  commissions.calculated
    |
    v
[4] CONSOLIDATOR  saves the result and counts what has come through
    |
    v
[5] NOTIFIER      reports back to the agency, exactly once
```

The stages run as separate processes, because each one is limited by a different resource. The
lookup stage queries the database for every row and spends most of its time waiting. The
calculator does arithmetic and spends all of its time on the processor. Separating them lets
you run more copies of whichever stage is the bottleneck, and it lets the calculator be tested
without a database at all.

Stage 1 writes every row into a staging table before it publishes a single message. If the file
is malformed part of the way through, or the process dies while reading it, the rows already
read are safe. The work resumes without asking the agency to upload the file again.

Stage 3 does no input or output of any kind, and that absence is deliberate. A stage that only
does arithmetic returns the same answer every time it runs, which makes it easy to test and
safe to repeat.

---

## 4. One message per row

> **As whoever is on support, I want one bad row to fail on its own, so that I am not
> unpicking five thousand good rows to find the one that broke.**

An earlier version of this pipeline grouped rows together, five thousand to a message, so that
a million-row file became two hundred messages instead of a million. That change was reversed,
and the reasoning is worth recording.

Grouping was meant to save the cost of writing to Kafka. It did not, because stage 2 queries
the database for every individual row, and those round trips happen whether the rows travel
together or separately. Grouping moved the database calls around without removing any of them.

Grouping made the handling of a bad row worse in three ways:

| One message per row         | Five thousand rows per message         |
|---------------------------|--------------------------------------|
| A bad row fails on its own  | One bad row spoils the whole group     |
| Retry one row               | Retry all five thousand                |
| Progress visible row by row | Progress jumps five thousand at a time |

Grouping is worth it only when the cost you are avoiding genuinely happens once per group. When
the cost is paid for every item regardless, grouping buys you nothing and leaves you with a
harder job when something goes wrong.

---

## 5. When the same message arrives twice

> **As a commissions agency, I want to be paid the right amount once, so that a retry
> somewhere inside the system never turns into commission counted twice.**

Kafka delivers each message at least once, which means sometimes more than once. A consumer can
read a message, complete the work, and then die before recording how far it got. That message
is then delivered again to whichever consumer takes over. This is normal behaviour, and the
pipeline is built to cope with it rather than prevent it.

Every result row is keyed on the row identifier together with the batch identifier, and writes
overwrite rather than insert. Processing the same message twice therefore writes the same row
twice with identical values, and the second write replaces the first. Nothing is counted twice.

The order of these two operations matters, and it is the same throughout the pipeline:

```
write to the database    THEN    record the position in Kafka
```

A process that dies between those two steps repeats the work when it restarts, which is
harmless because the write overwrites. Reversing the order is not harmless. A crash can then
leave the position recorded while the work was never done, and that row is lost with nothing to
indicate it. The pipeline therefore writes first and accepts that some work will be done twice,
because the upsert makes repeated work harmless and nothing makes a lost row recoverable.

Turning off automatic commit stops the client committing on a timer. It does not stop the
client tracking the position of every message it hands you, and closing the consumer commits
every position it has tracked. Two separate settings therefore govern the ordering above, and
turning off only the automatic commit is the easy mistake. Both the automatic commit and the
automatic position tracking have to be turned off, otherwise the code commits positions it
never intended to commit. This codebase had only the first one turned off, and five rounds of
review missed it, because each round read the automatic commit setting and looked no further.

---

## 6. Knowing a batch is finished

> **As a commissions agency, I want to hear that my batch is done only when every row in it
> really has been processed, so that I am not looking at half a report.**

Each batch records how many rows the file contained when it was read. The batch is finished
once the number of results written for it matches that number.

The check and the update happen in a single statement:

```sql
UPDATE Batches
SET    Status = 'Complete', CompletedAt = SYSDATETIMEOFFSET()
WHERE  BatchId = @batchId
  AND  Status <> 'Complete'
  AND  (SELECT COUNT(*) FROM ProcessedRows WHERE BatchId = @batchId) = ExpectedRowCount
```

Counting first and updating afterwards would leave a gap between the two operations. A second
process could run its count inside that gap, and both processes would then conclude that they
were the one that finished the batch. Moving the condition inside the update removes the gap,
because the database evaluates the condition and applies the change in a single operation. The
process that gets one affected row back is the one that finished the batch. Every other process
gets zero and does nothing.

Holding the count in memory would save a query, which makes it tempting. An in-memory count
does not survive a restart, however, and it gives the wrong answer as soon as a second instance
starts up.

---

## 7. Telling the agency once, and only once

> **As a commissions agency, I want exactly one message telling me my batch is finished. Two
> makes me doubt the system. None makes me ring up and ask.**

An email that has gone out cannot be recalled, so sending a notification twice leaves a
permanent second message. Notification is therefore the one step in this pipeline that
repeating the work cannot make safe, and it is built differently from every other step.

### The obvious approach, and why it fails

The obvious approach is to claim the batch first and send afterwards:

```sql
UPDATE Batches SET NotifiedAt = SYSDATETIMEOFFSET()
WHERE  BatchId = @batchId AND Status = 'Complete' AND NotifiedAt IS NULL
```

One affected row means this process is the one that should send. Zero means another process
already has. The database decides which process sends, and no lock is needed anywhere.

The claim itself is decided correctly, but the approach does not survive a crash between
claiming and sending. A process can claim the batch and then die before it sends anything. The
claim has already committed, so every other process reads the batch as claimed and leaves it
alone, and the agency is never told.

Reversing the order trades one failure for another. The notification goes out, the process dies
before recording the claim, and the next process sends a second copy.

Both orderings leave a window in which a crash breaks the guarantee, and reordering the two
steps only moves the window rather than closing it. Two steps that must either both happen or
neither happen cannot be made safe by ordering alone, which is the two generals problem.

### Writing down the intention instead

The fix is to record the intention to send inside the same transaction as the database write,
instead of treating the send as a separate step that follows it:

```
BEGIN TRANSACTION
    mark the batch complete
    write a row into OutboxMessages describing what needs to be sent
COMMIT
```

Either both of those happen or neither does. Once the transaction commits, the intention to
notify is a durable fact stored beside the data it describes. A separate process reads the
waiting rows and performs the send, retrying until it succeeds.

The send can now fail any number of times, because the record of what needs sending outlives
every attempt.

### Why the receiving end checks as well

> **As whoever is on support, I want a duplicate to be impossible rather than unlikely, so
> that I am not apologising to an agency for a second email.**

A process can send the notification successfully and then die before recording that it did.
Nothing knows the send happened, so the row is retried and the agency receives a second
notification. The duplicate is produced by the retry itself, so no change to the retry can
remove it, and the protection has to be enforced at the receiving end.

The receiving end therefore enforces uniqueness as well. The identifier on the notification
document is derived from the batch identifier rather than newly generated. When a retry arrives
carrying an identifier the store has already seen, the store rejects it.

Two separate checks now guard the send, and each covers a failure the other cannot:

| Check          | Where it lives                      | What it stops                       |
|--------------|-----------------------------------|-----------------------------------|
| The claim      | The sender, in SQL                  | Two processes both deciding to send |
| The identifier | The receiver, in the document store | A resend after dying mid-send       |

The general rule is that any sender which retries needs a duplicate check on the receiving end,
because the sender can always die between sending and recording that it sent.

### Why a lease and not a flag

> **As whoever is on support, I want a process that died mid-job to clean up after itself, so
> that I am not clearing stuck records by hand at two in the morning.**

A process that picks up an outbox row takes a lease with a time limit on it, rather than
setting a flag that marks the row as being worked on.

A process sets the flag and can then crash, and the row stays flagged until somebody notices
and clears it by hand. A lease expires on its own. When the process holding it dies, the row
becomes available again without anyone intervening.

The lease stops two processes doing the same work at the same time, but it does not by itself
make the design correct. A lease can expire while the process holding it is still running and
merely slow, and both processes will then send. The two checks in the table above are what make
a double send harmless, and the correctness of the design rests on those two checks rather than
on the lease.

Retry timing is stored on the row rather than held in memory. If it were held in memory, a
restart would reset every pending retry to fire immediately. Restarts usually follow an outage,
so the retries would all fire at once, against a service that has just come back and is least
able to take the load.

Each delay also carries a small random adjustment. Without it, every message that failed at the
same moment would retry at the same moment, and would keep colliding on every attempt after
that.

---

## 8. Dates, and why a row identifier is not enough

> **As a member of the finance team closing off the month, I want to know which accounting
> period every commission belongs to, so that I can reconcile the month and be confident
> nothing has drifted between periods.**

Before this was added, the pipeline verified two things: every row carried an identifier, and
the number of rows processed matched the number expected. That is not sufficient for a
settlement system, because neither check asks what point in time the work applies to.

Commission belongs to an accounting period. The same policy can earn commission in March and
again in April. A row identifier cannot tell you which month a figure belongs to. Only a date
can. A file arriving on the third of the month may correctly contain rows that were effective
on the twenty-eighth of the month before. Deciding how far back that is allowed to go is a
business decision, and it needs a written rule.

Four distinct dates are involved, and most of the work is in keeping them apart:

| What it is        | What it means                      | In this pipeline                |
|-----------------|----------------------------------|-------------------------------|
| Effective date    | When it was true in the real world | The premium transaction date    |
| Recorded date     | When this system found out         | When the row was processed      |
| Accounting period | Which month it settles in          | Derived from the effective date |
| As-of date        | Which reference data was used      | Which rate was applied          |

A single date column can hold only one of the four meanings in the table above, which is why
collapsing all four into one column is the usual mistake. The loss stays hidden until somebody
asks a question the column was never able to answer. The example that comes up most often is a
row processed in April against a March effective date, where the March rate is later found to
be wrong and corrected. Somebody then has to find which rows were calculated using the old
rate, and one date column records nothing about which rate each row used. With these four,
every affected row can be found and corrected, and no others are touched.

Keeping the four dates apart matters for a second reason, which is that re-running a
calculation is safe today only because the calculation reads values that never change. That is
true because the rates happen to be fixed, not because anything in the design guarantees it. If
rates ever vary over time, a replay would recompute an old row using today's rate, and the
recomputed figure would carry no marker distinguishing it from a correctly calculated one.
Selecting the rate by effective date makes replay safe by construction instead.

> **As a member of the finance team, I do not want anything written into a month I have
> already closed and reported on, so that a late row becomes a decision somebody makes rather
> than something the system does quietly.**

Four checks on the effective date follow from the rule that a closed month must not be written
into.

**Is there a sensible date**

The effective date has to parse, and it cannot be implausibly old or set in the future. A
commission dated next year is almost always a mistyped year. Rows failing this check are
rejected when the file is read.

**Work out the period**

The accounting period is derived from the effective date once, when the file is read. This step
rejects nothing. Its only requirement is that the derived value is stored and then read back
rather than worked out a second time, because a figure derived twice can differ the second
time.

**Is the period still open**

The month has to still be open. A row belonging to a month that has been closed and reported on
is rejected. That rejection is a business decision rather than a technical error, so it reads
differently in the logs from something that simply broke.

**Does the batch hang together**

Every row in one file should belong to the same month. When they do not, the batch is flagged
for a person to review rather than split up automatically.

A file is the unit an agency reconciles against, so a file covering two different months is
more likely to be a mistake than an intention. The batch-consistency check therefore rests on
how the commissions business works rather than on a technical rule. It is flagged rather than
refused, because somebody correcting a mistyped date can legitimately move a row into a
different month, and refusing the batch outright would block the correction the system needs to
accept.

---

## 9. What happens when things break

> **As whoever is on support, I want every failure to be either self-correcting or obvious,
> so that I can tell at a glance whether something needs me.**

**One consumer stops**

Its partitions move to another instance, and anything uncommitted is delivered again. The cost
is some repeated work.

**A consumer dies partway through writing**

The position was never recorded, so the messages return and the rows are overwritten. Nothing
is counted twice.

**The database goes down**

Consumers stop making progress and their positions stay where they are. Work resumes from that
point when the database returns.

**The process dies between marking a batch complete and recording the notification**

Both operations sit in one transaction, so neither took effect. The batch remains waiting to be
completed, and it is picked up again.

**The process dies after sending but before recording that it sent**

The row is retried, and the receiving end rejects the duplicate because the identifier is
already present. The agency does not receive a second notification.

**A message that can never be processed**

The message is retried and keeps failing, and it blocks its partition while it does. Nothing on
that partition moves until somebody removes or fixes the message. Section 10 records this as
the next thing to build.

**The notification store is down**

The retry delay grows with each attempt, and the row is eventually set aside for somebody to
look at. Nothing retries continuously in the meantime.

The timestamps used for leases come from the database rather than from the application. Two
machines whose clocks are slightly out of step would disagree about when a lease expired, and
one of them would take a lease that the other still holds. Reading a single clock removes the
problem.

---

## 10. What is not built, and why

> **As the next developer on this system, I want the known gaps written down with the
> reasoning, so that I can tell a deliberate trade-off from something that was forgotten.**

**Somewhere to put Kafka messages that cannot be processed**

A malformed message currently blocks its partition, and nothing on that partition moves until
somebody intervenes. The outbox in section 7 gives the notification path somewhere to put a
message it cannot handle. The consumer path has no equivalent. This is the next thing to build.

**Telling an agency about a batch that still has rows needing attention**

A batch records how many rows parsed and how many were rejected, but completion counts only the
parsed ones. A file where three rows need fixing is therefore reported as finished while those
three remain outstanding. The agency should instead be told how many rows were processed and
how many need attention, and hear nothing further until the outstanding rows are resolved.

**Picking up batches that completed before the outbox existed**

A batch is marked complete once, and the guard that makes that safe also prevents it being
marked complete a second time. Batches finished before notifications were recorded this way
therefore have nothing queued to send, and never will. A slower background scan should find
them and queue one.

**Reading the database transaction log to drive the outbox**

Polling for waiting rows costs a query per interval and adds latency up to one interval.
Reading the transaction log avoids both, but it requires infrastructure this project does not
have. Polling is the right trade at this size.

**Actually sending email**

Notifications are written as documents instead. The mechanism, the risk of sending twice and
the check against it are all the same, and no mail server is needed. If a real email provider
were used, its idempotency key would serve the same purpose as the document identifier.

**Moving the database work off the consumer thread**

The database work runs on the consumer thread today. If a write takes too long, the consumer
waits too long between reads and the group evicts it, and the evicted consumer carries on
writing for partitions it no longer owns. This only happens during a sustained database outage,
and the result is confusion over which consumer owns what rather than any loss of data.

**Clearing out old outbox rows**

Outbox rows are inserted and never deleted, so the table only ever grows. Sent rows will need
archiving before this pipeline runs in production for any length of time. Until then, the
growth does not slow the active path, because the index covers only the rows that are still
waiting.

**Rates that change over time**

The rate table carries no validity dates, so a recorded as-of date indicates when the rate was
read rather than which version was used. Section 8 explains why that distinction matters. The
column exists so the stronger version can be added later without unpicking the design.

Each of these is a decision that was made knowing what it costs, not something that was
overlooked. Several are tracked as issues on this repository.
