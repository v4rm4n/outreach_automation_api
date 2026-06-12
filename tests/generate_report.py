# --- outreach_automation_api/tests/generate_report.py ---

import asyncio
from datetime import timezone
from services import MONGO, ECHO

async def generate_metrics_report():
    await MONGO.connect()
    db = MONGO.get_db()

    ECHO.info("Gathering system metrics from MongoDB...[/]")

    # 1. Status Counts
    pipeline = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
    status_counts = await db["dispatch_jobs"].aggregate(pipeline).to_list(length=None)
    counts = {item["_id"]: item["count"] for item in status_counts}
    total_jobs = sum(counts.values())

    # 2. Processing Throughput
    sent_jobs = await db["dispatch_jobs"].find({"status": "sent"}).sort("processed_at", 1).to_list(length=None)
    
    throughput_per_minute = 0
    if len(sent_jobs) > 1:
        first_job_time = sent_jobs[0]["processed_at"].replace(tzinfo=timezone.utc)
        last_job_time = sent_jobs[-1]["processed_at"].replace(tzinfo=timezone.utc)
        total_seconds = (last_job_time - first_job_time).total_seconds()
        
        if total_seconds > 0:
            throughput_per_minute = (len(sent_jobs) / total_seconds) * 60

    # 3. Print the Executive Report
    print("\n" + "="*55)
    print(" 📊 SYSTEM SCALABILITY & METRICS REPORT")
    print("="*55)
    print(f"Total Jobs Ingested  : {total_jobs}")
    print(f"✅ Successfully Sent : {counts.get('sent', 0)}")
    print(f"⏳ Backlog (Pending) : {counts.get('pending', 0)} (Safely held in RabbitMQ)")
    print(f"❌ Failed (DLX)      : {counts.get('failed', 0)}")
    print("-" * 55)
    print(f"⚙️ Worker Throughput : ~{throughput_per_minute:.1f} messages / minute")
    
    # The ultimate proof for the interview:
    if throughput_per_minute > 0:
        max_daily_capacity = (throughput_per_minute * 60 * 24)
        print(f"📈 Max Daily Capacity: ~{max_daily_capacity:,.0f} messages / day (per worker)")
    print("="*55 + "\n")

    await MONGO.close()

if __name__ == "__main__":
    asyncio.run(generate_metrics_report())