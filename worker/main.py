# - outreach_automation_api/worker/main.py -

import os
import re
import asyncio
import json
from datetime import datetime, timezone
from bson import ObjectId

from config import APPCFG, APICFG
from services import configure_logging, load_topology_config
from services import ECHO, MONGO, RABBIT, REDIS

configure_logging(
    log_level=APICFG["LOG_LEVEL"],
    dev=APPCFG["DEV_MODE"]
)

def render_template(body: str, creator: dict) -> str:
    """Replaces {{field}} in the template with actual creator data."""
    # Find all {{keys}} in the text
    matches = re.findall(r"\{\{(.*?)\}\}", body)
    rendered = body
    for match in matches:
        # Fallback to "there" if the field is missing (e.g. "Hi there,")
        val = creator.get(match.strip(), "there")
        rendered = rendered.replace(f"{{{{{match}}}}}", str(val))
    return rendered

async def process_campaign_batch(message):
    """Consumes the RabbitMQ batch payload and processes the dispatch jobs."""
    async with message.process(): 
        # By entering this context block, aio_pika will automatically ACK the message 
        # when the block completes, or NACK it if an unhandled Exception is raised.
        
        try:
            payload = json.loads(message.body.decode())
            campaign_id = payload.get("campaign_id")
            job_id_strs = payload.get("job_ids", [])
            
            if not job_id_strs:
                return

            db = MONGO.get_db()
            job_object_ids = [ObjectId(jid) for jid in job_id_strs]

            # 1. Fetch only "pending" jobs. This ensures IDEMPOTENCY.
            # If RMQ accidentally delivers this twice, we won't double-send.
            pending_jobs = await db["dispatch_jobs"].find({
                "_id": {"$in": job_object_ids},
                "status": "pending"
            }).to_list(length=None)

            if not pending_jobs:
                ECHO.debug(f"Batch for campaign {campaign_id} has no pending jobs. Skipping.")
                return

            # 2. Fetch the Campaign & Template Blueprint
            campaign = await db["campaigns"].find_one({"_id": ObjectId(campaign_id)})
            if not campaign:
                raise ValueError(f"Campaign {campaign_id} not found.")

            template = await db["templates"].find_one({"_id": ObjectId(campaign.get("template_id"))})
            if not template:
                raise ValueError(f"Template not found for campaign {campaign_id}.")
            template_body = template.get("body", "")

            # 3. Fetch all related Creators in one fast query
            creator_ids = [ObjectId(job["creator_id"]) for job in pending_jobs]
            creators_cursor = await db["creators"].find({"_id": {"$in": creator_ids}}).to_list(length=None)
            creators_map = {str(c["_id"]): c for c in creators_cursor}

            ECHO.info(f"Processing {len(pending_jobs)} jobs for Campaign {campaign_id}...")

            # 4. The Dispatch Loop
            for job in pending_jobs:
                job_id = job["_id"]
                creator = creators_map.get(job["creator_id"])
                
                if not creator:
                    # Atomic Failure Update
                    await db["dispatch_jobs"].update_one(
                        {"_id": job_id},
                        {"$set": {"status": "failed", "error_message": "Creator deleted or missing"}}
                    )
                    continue

                # Generate the final string
                final_message = render_template(template_body, creator)

                try:
                    # ==========================================
                    # TODO: 1. Await Redis Token Bucket Check Here
                    # TODO: 2. Await Instagram API Dispatch Here
                    # ==========================================
                    
                    # SIMULATED SEND (Replace later)
                    ECHO.debug(f"--> Sending to @{creator.get('handle')}: {final_message[:30]}...")
                    await asyncio.sleep(0.5) # Simulating network IO
                    
                    # Atomic Success Update
                    # We include "status": "pending" in the filter to prevent race conditions
                    await db["dispatch_jobs"].update_one(
                        {"_id": job_id, "status": "pending"},
                        {"$set": {
                            "status": "sent", 
                            "processed_at": datetime.now(timezone.utc)
                        }}
                    )
                except Exception as dispatch_err:
                    # Only fail this specific message, don't crash the batch!
                    ECHO.error(f"Failed to send to {creator.get('handle')}: {dispatch_err}")
                    await db["dispatch_jobs"].update_one(
                        {"_id": job_id},
                        {"$set": {
                            "status": "failed", 
                            "error_message": str(dispatch_err)
                        }}
                    )

            ECHO.info(f"Batch completed for Campaign {campaign_id}.")

        except Exception as e:
            # Infra-level errors (Mongo timeout, bad JSON) will fall down here.
            # Raising it forces a NACK in RabbitMQ so the batch is preserved and retried.
            ECHO.error(f"Critical error processing RMQ batch: {e}")
            raise


async def run_worker_loop():
    """Binds to the queue and listens for messages forever."""
    channel_pool = RABBIT.get_channel_pool()
    
    async with channel_pool.acquire() as channel:
        await channel.set_qos(prefetch_count=10)
        queue = await channel.get_queue("dispatch_jobs", ensure=False)
        
        ECHO.info("[cyan]Worker is actively listening to 'dispatch_jobs' queue...[/]")
        await queue.consume(process_campaign_batch)
        await asyncio.Future() 

async def main():
    try:
        await MONGO.connect()
        await RABBIT.connect()
        topology_cfg = load_topology_config("topology.yaml")
        await RABBIT.setup_topology(topology_cfg)
        await REDIS.connect()
    except RuntimeError:
        ECHO.error("Resource initialization failed")
        os._exit(1)
        
    try:
        ECHO.info("`main` routine initiated!")
        await run_worker_loop()
    finally:
        await MONGO.close()
        await RABBIT.close()
        await REDIS.close()
    
if __name__ == "__main__":
    asyncio.run(main())