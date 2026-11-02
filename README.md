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

### 3. Worker Pool — Job Execution and Retry

The Worker Pool consumes jobs from the RabbitMQ priority queue and atomically claims them in PostgreSQL using leases and fencing tokens.

Each worker executes the registered job handler in a separate process while a heartbeat thread periodically renews the lease.

On successful execution, the worker persists the result and marks the job as SUCCEEDED. On handler failure or timeout, it schedules a retry with exponential backoff or marks the job as DEAD-LETTERED when retries are exhausted.

If ownership is lost, the worker terminates handler execution and requeues the message. RabbitMQ messages are acknowledged only after the corresponding database state transition succeeds.

```mermaid
flowchart TD
    Start["Worker receives RabbitMQ message"] --> Parse["Parse message and extract job_id"]
    Parse --> Valid{"Valid message?"}

    Valid -->|"No"| Invalid["Log invalid message"]
    Invalid --> AckInvalid["ACK message"]

    Valid -->|"Yes"| Claim["Atomically claim job in PostgreSQL"]
    Claim --> Claimed{"Claim successful?"}

    Claimed -->|"No"| Log["Log: already claimed or completed"]
    Log --> AckDuplicate["ACK message"]

    Claimed -->|"Yes"| Lease["Acquire lease; increment attempt_count and lock_version"]
    Lease --> StartHeartbeat["Start heartbeat thread"]
    StartHeartbeat --> Execute["Execute handler in child process with timeout"]

    StartHeartbeat -.-> Renew["Periodically renew lease"]
    Renew -.-> Renewed{"Lease renewal successful?"}
    Renewed -->|"Yes"| Renew
    Renewed -->|"No"| Lost["Set ownership_lost event"]
    Lost -.-> Terminate["Terminate handler process"]

    Execute --> Outcome{"Handler execution outcome?"}
    Outcome -->|"Success"| StopHeartbeat["Stop heartbeat and join thread"]
    Outcome -->|"Error / Timeout"| StopHeartbeat
    Outcome -->|"Ownership lost"| SetLost["Set ownership_lost event"]
    SetLost --> StopHeartbeat

    StopHeartbeat --> CheckOwnership{"Ownership lost?"}
    CheckOwnership -->|"Yes"| NackLost["NACK with requeue"]
    CheckOwnership -->|"No"| Result{"Handler result?"}

    Result -->|"Error / Timeout"| Retry["Schedule retry or dead-letter"]
    Retry --> RetryOutcome{"Database transition successful?"}
    RetryOutcome -->|"Retry scheduled"| AckRetry["ACK message"]
    RetryOutcome -->|"Dead-lettered"| AckRetry
    RetryOutcome -->|"Update rejected / failed"| NackRetry["NACK with requeue"]

    Result -->|"Success"| Complete["Persist result and mark SUCCEEDED"]
    Complete --> Fenced{"Fenced completion accepted?"}
    Fenced -->|"Yes"| AckSuccess["ACK message"]
    Fenced -->|"No"| NackComplete["NACK with requeue"]

    Terminate -.-> StopHeartbeat
```
