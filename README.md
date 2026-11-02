## Architecture

### 1. Job API — Submission and Management

The Job API accepts jobs, persists job and outbox records
atomically, and exposes operational endpoints.

```mermaid
flowchart TD
    Client["Client"]

    subgraph API["FastAPI — Job API"]
        Submit["POST /api/jobs"]
        Status["GET /api/jobs/{job_id}"]
        Depth["GET /api/jobs/queue-depth"]
        Cancel["POST /api/jobs/{job_id}/cancel"]
        Drain["POST /api/jobs/drain"]
    end

    subgraph DB["PostgreSQL"]
        Jobs[("Jobs Table")]
        Outbox[("Outbox Events Table")]
    end

    Client --> Submit
    Client --> Status
    Client --> Depth
    Client --> Cancel
    Client --> Drain

    Submit --> Validate["Validate request and job type"]
    Validate --> Create["Create Job: QUEUED"]
    Create --> CreateEvent["Create OutboxEvent: unpublished"]
    Create --> TX["Commit both in one transaction"]
    CreateEvent --> TX
    TX --> Response["Return job_id and QUEUED"]
    Response --> Client

    Status --> ReadJob["Read job by ID"]
    ReadJob --> Jobs
    ReadJob --> StatusResponse["Return status, attempts, error and result"]
    StatusResponse --> Client

    Depth --> Aggregate["Aggregate QUEUED and RUNNING jobs"]
    Aggregate --> Jobs
    Aggregate --> DepthResponse["Return counts by status and priority"]
    DepthResponse --> Client

    Cancel --> LockJob["Lock job row"]
    LockJob --> CheckCancel{"Job is QUEUED?"}
    CheckCancel -->|"Yes"| MarkCancel["Set status = CANCELLED"]
    MarkCancel --> CommitCancel["Commit transaction"]
    CommitCancel --> CancelResponse["Return CANCELLED"]
    CheckCancel -->|"No"| Conflict["Return HTTP 409"]
    CancelResponse --> Client
    Conflict --> Client

    Drain --> BulkUpdate["Update all QUEUED jobs to CANCELLED"]
    BulkUpdate --> CommitDrain["Commit transaction"]
    CommitDrain --> DrainResponse["Return cancelled_count"]
    DrainResponse --> Client
```

### 2. Dispatcher — Recovery and Message Publishing

The dispatcher runs in a polling loop and performs two responsibilities:

- Recovers jobs whose worker leases have expired.
- Publishes due, unpublished outbox events to the RabbitMQ priority queue.

```mermaid
flowchart TD
    Start["Dispatcher starts"] --> Loop["Dispatcher polling loop"]

    Loop --> Recovery["Recover expired RUNNING jobs"]
    Recovery --> Find["Find jobs where lease_expires_at <= current time"]
    Find --> Found{"Expired jobs found?"}

    Found -->|"No"| Dispatch
    Found -->|"Yes"| RetryCheck{"attempt_count > max_retries?"}

    RetryCheck -->|"Yes"| DLQ["Mark job DEAD-LETTERED"]
    RetryCheck -->|"No"| Requeue["Set job status = QUEUED"]
    Requeue --> NewEvent["Create new OutboxEvent"]
    DLQ --> Dispatch
    NewEvent --> Dispatch

    Dispatch["Dispatch pending outbox events"] --> Fetch["Fetch unpublished, due events for QUEUED jobs"]
    Fetch --> Events{"Events found?"}

    Events -->|"No"| Wait["Wait for polling interval"]
    Events -->|"Yes"| Publish["Publish job_id to RabbitMQ priority queue"]
    Publish --> Confirm{"Publisher confirm received?"}

    Confirm -->|"Yes"| MarkPublished["Set published_at on OutboxEvent"]
    MarkPublished --> Commit["Commit database transaction"]
    Commit --> More{"More events to dispatch?"}
    More -->|"Yes"| Fetch
    More -->|"No"| Wait

    Confirm -->|"No / Error"| Rollback["Rollback transaction"]
    Rollback --> Wait

    Wait --> Loop
```
