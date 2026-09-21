# Design

This document describes how the commissions pipeline works and why it is built this way.
Each section starts with the user story it exists to satisfy, so that anyone reading the
code can see what a piece of it is for before reading how it works.

## When this was written

Stages 1 to 4 were built first and this document describes them as they actually work, so
parts of it were written after the code rather than before it. Stage 5 and the outbox in
sections 7 and 8 are the other way round. Those are specified here first and are not built
yet.

That is worth stating plainly, because a design document that quietly suggests it all came
first is not worth much. Where something is not built, it says so.

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

There are four of them, and most of the design decisions in this document trace back to one
of these.

**The commissions agency** uploads a file of policies and wants to know what it earned.

**The finance team** closes off the month and has to be able to stand behind the numbers.

**Whoever is on support** gets called when something breaks, usually at an inconvenient hour.

**The next developer** picks this up and needs to understand why it is shaped like this.

---

## 2. What the pipeline has to do

> **As a commissions agency, I want to upload my file of policies and get on with my day, so
> that I am not sitting watching a progress bar while a million rows are processed.**

An agency uploads a CSV file. Every row is one policy with a premium amount and a commission
rate on it. The pipeline works out what commission is owed on each row, saves the result, and
reports back when the whole file is done.

The upload finishes as soon as the rows are safely stored. Everything after that happens in
the background.

Two things make this harder than it sounds.

The first is volume. One agency can send a million rows and there are hundreds of agencies.
Something that works fine for fifty rows can fall over at a million, and it usually falls
over because it does something once per row that should have been done once for the whole
file.

The second is the notification at the end. Everywhere else in this pipeline, safety comes
from being able to do the same thing again, because writing the same result twice leaves you
with the same answer. An email cannot be unsent. That one difference drives most of what
happens in sections 6 and 7.

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

The stages are separate processes because they behave differently. The lookup stage hits the
database for every row and spends most of its life waiting for it. The calculator does
arithmetic and spends all of its life using the processor. Keeping them apart means running
more of whichever one is the bottleneck, and it means the calculator can be tested without a
database anywhere near it.

Stage 1 writes every row into a staging table before publishing a single message. If the file
turns out to be malformed halfway down, or the process dies, the data is already safe and the
work can be picked up again without asking the agency to upload it a second time.

Stage 3 does not read or write anything at all. That is on purpose. A stage that only does
arithmetic gives the same answer every time it runs, which makes it easy to test and safe to
repeat.

---

## 4. One message per row

> **As whoever is on support, I want one bad row to fail on its own, so that I am not
> unpicking five thousand good rows to find the one that broke.**

An earlier version grouped rows together, five thousand to a message, so that a million-row
file became two hundred messages instead of a million. That was changed back, and the reason
is worth recording.

The assumption behind grouping was that it saved the cost of writing. It did not. Stage 2
looks up the broker in the database for every single row, so that cost gets paid whether the
rows travel together or separately. Grouping them moved the database calls around without
getting rid of any of them.

What grouping did change was what happens when one row is bad, and it made that worse:

| One message per row         | Five thousand rows per message         |
|-----------------------------|----------------------------------------|
| A bad row fails on its own  | One bad row spoils the whole group     |
| Retry one row               | Retry all five thousand                |
| Progress visible row by row | Progress jumps five thousand at a time |

The general lesson is that grouping work only pays off when the cost being avoided is
genuinely per group. If the expensive part happens per item anyway, you have paid for the
grouping and got nothing back except a harder time when something goes wrong.

---

## 5. When the same message arrives twice

> **As a commissions agency, I want to be paid the right amount once, so that a retry
> somewhere inside the system never turns into commission counted twice.**

Kafka delivers a message at least once, which means sometimes more than once. If a consumer
reads a message, does the work, and then dies before recording how far it got, that message
comes back again to whoever picks up the work. That is normal, and this pipeline is built to
expect it rather than to prevent it.

Every result row is keyed on the row identifier together with the batch identifier, and
writes overwrite rather than insert. Processing the same message twice writes the same row
twice with the same numbers in it, and the second write replaces the first. Nothing is added
up twice.

The order of these two operations matters, and it is the same everywhere in the pipeline:

```
write to the database    THEN    record the position in Kafka
```

Dying between those two means the work is done again on restart, which is harmless because
the write overwrites. Doing it the other way round means a crash can record that the work was
finished when it was not, and that row is gone without anyone knowing. One order costs
repeated work. The other costs data.

There is one setting here that is easy to get wrong. Turning off automatic commit only does
half the job. The client still keeps track of the position of every message it hands you, and
closing the consumer commits whatever it has been keeping track of. Both settings have to be
turned off for a manual commit to actually be manual. This was wrong in this codebase through
five rounds of review, because each round read the first setting and assumed it meant what it
says.

---

## 6. Knowing a batch is finished

> **As a commissions agency, I want to hear that my batch is done only when every row in it
> really has been processed, so that I am not looking at half a report.**

Each batch records how many rows were in the file when it was read. The batch is finished
when the number of results written for it matches that number.

The check and the update happen in one statement:

```sql
UPDATE Batches
SET    Status = 'Complete', CompletedAt = SYSDATETIMEOFFSET()
WHERE  BatchId = @batchId
  AND  Status <> 'Complete'
  AND  (SELECT COUNT(*) FROM ProcessedRows WHERE BatchId = @batchId) = ExpectedRowCount
```

Counting first and then updating would leave a gap between the two where another process
could do exactly the same thing, and both would come away believing they were the one that
finished the batch. Putting the condition inside the update closes that gap, because the
database works it out and applies the change in one go. Whoever gets one row back is the one
that won.

Keeping the count in memory instead is tempting because it saves a query. It does not survive
a restart, and it is wrong the moment a second instance runs.

---

## 7. Telling the agency once, and only once

> **As a commissions agency, I want exactly one message telling me my batch is finished. Two
> makes me doubt the system. None makes me ring up and ask.**

**Not built yet. This is the design.**

This is the part that cannot be made safe by simply doing it again, so it works differently
from everything else.

### The obvious approach, and where it lets you down

The obvious approach is to claim the batch and then send:

```sql
UPDATE Batches SET NotifiedAt = SYSDATETIMEOFFSET()
WHERE  BatchId = @batchId AND Status = 'Complete' AND NotifiedAt IS NULL
```

One row back means this process is the one that should send. Nothing back means somebody else
already has. The database sorts out who wins and there is no need for a lock.

That part is sound. The problem is what happens if the process dies in between claiming and
sending. The claim worked, so nothing will ever try again, and the agency never hears
anything. Swapping the order round swaps the problem: the notification goes out, the claim is
never recorded, and the next process along sends a second one.

Neither order is safe on its own. This is the two generals problem, and picking a better
order does not get you out of it.

### Writing down the intention instead

What does get you out of it is to stop treating the send as something that happens next to
the database write, and instead record the intention to send as part of the same transaction:

```
BEGIN TRANSACTION
    mark the batch complete
    write a row into OutboxMessages describing what needs to be sent
COMMIT
```

Either both of those happen or neither does. Once that transaction commits, the intention to
notify is a durable fact sitting in the same database as the data it refers to. A separate
process reads the rows that are waiting and does the actual sending, retrying until it gets
through.

The sending can now fail as often as it likes, because the record of what needs sending
outlives all of it.

### Why the receiving end checks as well

> **As whoever is on support, I want a duplicate to be impossible rather than unlikely, so
> that I am not apologising to an agency for a second email.**

Retrying does not remove the duplicate problem, it moves it. A process can send successfully
and then die before recording that it did. So the receiving end checks too. The identifier on
the notification document is derived from the batch identifier rather than generated fresh,
so a second attempt to write the same notification is rejected by the store itself.

That gives two separate checks which fail in different places:

| Check          | Where it lives                      | What it stops                       |
|----------------|-------------------------------------|-------------------------------------|
| The claim      | The sender, in SQL                  | Two processes both deciding to send |
| The identifier | The receiver, in the document store | A resend after dying mid-send       |

The general version is that anything which retries needs a duplicate check the receiver
enforces, because the sender can always die at the worst possible moment.

### Why a lease and not a flag

> **As whoever is on support, I want a process that died mid-job to clean up after itself, so
> that I am not clearing stuck records by hand at two in the morning.**

When a process picks up an outbox row to work on, it takes a lease with a time limit rather
than setting a flag that says it is working on it.

A flag gets set by a process that might then crash, and the row sits there flagged forever
waiting for somebody to notice and clear it. A lease runs out. If whoever held it died, the
row becomes available again on its own and nobody has to do anything.

The lease exists to stop two processes doing the same work at the same time. It is not what
makes the thing correct. If a lease runs out while the process holding it is still alive and
merely slow, two processes might both send, and that has to be harmless. It is, because of
the two checks above.

Retry timing lives on the row rather than in memory. Kept in memory, a restart would reset
every pending retry to "try now", so the restart caused by an outage would produce a burst of
traffic at exactly the moment the thing being called is least able to cope with it. A bit of
randomness is added to the delay for the same reason. Without it, everything that failed
together retries together, forever.

---

## 8. Dates, and why a row identifier is not enough

> **As a member of the finance team closing off the month, I want to know which accounting
> period every commission belongs to, so that I can reconcile the month and be confident
> nothing has drifted between periods.**

**Not built yet. This is the design.**

The pipeline currently checks two things: every row has an identifier, and the number of rows
processed matches the number expected. For a settlement system that is not enough, because
nothing in there ever asks what point in time any of it applies to.

Commission belongs to an accounting period. The same policy can earn commission in March and
again in April. A row identifier cannot say which month a figure belongs to. Only a date can.
A file arriving on the third of the month can quite legitimately contain rows that were
effective on the twenty-eighth of the month before, so whether that is acceptable is a
business question with a business rule sitting behind it.

Four different dates are tangled up in this, and most of the work is keeping them apart:

| What it is        | What it means                      | In this pipeline                |
|-------------------|------------------------------------|---------------------------------|
| Effective date    | When it was true in the real world | The premium transaction date    |
| Recorded date     | When this system found out         | When the row was processed      |
| Accounting period | Which month it settles in          | Derived from the effective date |
| As-of date        | Which reference data was used      | Which rate was applied          |

Squashing all of that into one date column is the usual mistake, and it does not hurt until
somebody asks a question the data can no longer answer. The one that catches people out: a
row was processed in April against a March effective date, and then the March rate turned out
to be wrong and was corrected. With one date column the question cannot even be asked
properly. With these four, every row that used the old rate can be found and exactly those
corrected.

There is a second reason this matters here specifically. Re-running a calculation is safe at
the moment because the calculation only uses values that do not change, so running it again
gives the same answer. That is true today by luck rather than by design. If rates ever start
varying over time, a replay would quietly work out an old row using today's rate and produce
a wrong answer that looks exactly like a right one. Picking the rate by effective date makes
replaying safe by construction rather than by accident.

> **As a member of the finance team, I do not want anything written into a month I have
> already closed and reported on, so that a late row becomes a decision somebody makes rather
> than something the system does quietly.**

Four checks come out of that:

**Is there a sensible date**

The effective date has to parse, and it cannot be wildly old or sitting in the future. A
commission dated next year is almost always a typo in the year. Rows that fail this are
rejected when the file is read.

**Work out the period**

The accounting period is derived from the effective date once, when the file is read, and
never recalculated afterwards. This one cannot really fail. The discipline is in never
recalculating it, because a value worked out twice can come out differently.

**Is the period still open**

The month has to be open rather than closed and already reported on. A row for a closed month
is rejected, and that is a business decision rather than a technical error, so it needs to
read differently in the logs from something that simply broke.

**Does the batch hang together**

Every row in one file should belong to the same month. If they do not, the batch is flagged
for a person to look at rather than split up automatically.

That last one is a judgement call about this particular business rather than a technical
rule. A file is what an agency reconciles against, so a file covering two different months is
more likely to be a mistake than something somebody intended. Refusing it is the cautious
choice, and it is what a finance team would expect.

---

## 9. What happens when things break

> **As whoever is on support, I want every failure to be either self-correcting or obvious,
> so that I can tell at a glance whether something needs me.**

**One consumer stops**

Its partitions move across to another instance and anything uncommitted is delivered again.
The cost is some repeated work and nothing else.

**A consumer dies partway through writing**

The position was never recorded, so the messages come back and the rows are overwritten.
Nothing gets counted twice.

**The database goes down**

Consumers stop making progress and their positions stay where they are. Work carries on from
that point when the database comes back.

**The process dies between marking a batch complete and recording the notification**

Both of those happen in one transaction, so neither of them happened. The batch is still
waiting to be completed and it gets picked up again.

**The process dies after sending but before recording that it sent**

The row is retried, and the receiving end rejects the duplicate because the identifier is
already there. The agency does not get a second notification.

**A message that can never be processed**

Attempts are counted, and after a few tries the message is set aside somewhere it can be
looked at. It does not block anything else.

**The notification store is down**

Retries slow themselves down, and eventually the row is set aside for somebody to look at.
Nothing spins away in a loop in the meantime.

The timestamps used for leases come from the database rather than from the application. Two
machines with clocks slightly out of step will disagree about when a lease has expired, and
one of them will take a lease somebody else is still holding. Reading one clock rather than
several makes that go away.

---

## 10. What is not built, and why

> **As the next developer on this system, I want the known gaps written down with the
> reasoning, so that I can tell a deliberate trade-off from something that was forgotten.**

**Somewhere to put Kafka messages that cannot be processed**

At the moment a malformed message stops its partition and it stays stopped. The outbox in
section 7 handles this for the notification side, but the consumer side still needs it. This
is the next thing to do.

**Reading the database transaction log to drive the outbox**

Polling for waiting rows costs a query every interval and adds a little delay. Reading the
log avoids both, but it needs infrastructure this project does not have. Polling is the right
trade at this size.

**Actually sending email**

Notifications are written as documents instead. Same mechanism, same risk of sending twice,
same check, and no mail server to run. A real email provider's idempotency key does the same
job as the document identifier.

**Moving the database work off the consumer thread**

Waiting too long between reads can get the consumer thrown out of the group, and it then
carries on writing for partitions it no longer owns. It only bites during a long database
outage, and what comes out of it is confusion about ownership rather than lost data.

**Clearing out old outbox rows**

The table grows and never shrinks. Sent rows need archiving before this runs anywhere for a
long stretch. The index only covers rows still waiting, so the growth does not slow anything
down in the meantime.

**Rates that change over time**

The rate table has no dates on it, so recording an as-of date says when the rate was read
rather than which version it was. Section 8 explains why that matters. The column is there so
this can be added later without pulling things apart.

Each of these was decided with the cost in view rather than forgotten. Several are tracked as
issues on this repository.
