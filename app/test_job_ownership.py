import uuid

from app.worker import claim_job, renew_lease, complete_job

job_id = input("Enter a queued job ID: ").strip()
worker_id = f"worker-{uuid.uuid4()}"

# 1. Claim the job
job = claim_job(job_id, worker_id)

if not job:
    print("Could not claim job")
    raise SystemExit(1)

lock_version = job["lock_version"]

print("Claimed:", job_id)
print("Lock version:", lock_version)

# 2. Renew lease
renewed = renew_lease(job_id, worker_id, lock_version)
print("Lease renewed:", renewed)

# 3. Complete job
completed = complete_job(
    job_id, worker_id, lock_version, {"message": "Job executed successfully"}
)

print("Job completed:", completed)
