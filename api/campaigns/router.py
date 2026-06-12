# - outreach_automation_api/api/campaigns/router.py -

from fastapi import APIRouter, Depends, HTTPException, status
from bson import ObjectId
from pymongo import InsertOne
from pymongo.errors import BulkWriteError
from datetime import datetime, timezone

from api.auth.auth_service import auth_service, TokenPayload
from services import MONGO, ECHO
from services.rabbit import RABBIT

from shared.models.campaign import (
    CampaignCreate, 
    CampaignDocument, 
    CampaignResponse, 
    DispatchJobDocument,
    AddCreatorsRequest,
    CampaignStatus,
    DispatchStatus
)

router = APIRouter(
    prefix="/campaigns",
    tags=["Campaigns"],
    dependencies=[Depends(auth_service.get_current_user)]
)

@router.post("", response_model=CampaignResponse, status_code=status.HTTP_201_CREATED)
async def create_campaign(
    request: CampaignCreate,
    user: TokenPayload = Depends(auth_service.get_current_user)
):
    db = MONGO.get_db()
    
    template = await db["templates"].find_one({
        "_id": ObjectId(request.template_id), 
        "user_id": user.sub
    })
    if not template:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Template not found")

    camp_doc = CampaignDocument(
        user_id=user.sub,
        template_id=request.template_id,
        name=request.name,
        total_creators=0,
        status=CampaignStatus.DRAFT,
        priority=request.priority
    )
    camp_result = await db["campaigns"].insert_one(camp_doc.model_dump(by_alias=True, exclude={"id"}))
    campaign_id = str(camp_result.inserted_id)

    return CampaignResponse(_id=campaign_id, **camp_doc.model_dump(exclude={"id"}))


@router.post("/{campaign_id}/creators", status_code=status.HTTP_200_OK)
async def add_creators_to_campaign(
    campaign_id: str,
    request: AddCreatorsRequest,
    user: TokenPayload = Depends(auth_service.get_current_user)
):
    db = MONGO.get_db()

    campaign = await db["campaigns"].find_one({
        "_id": ObjectId(campaign_id),
        "user_id": user.sub
    })
    if not campaign:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Campaign not found")

    try:
        creator_object_ids = [ObjectId(cid) for cid in request.creator_ids]
    except Exception:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Invalid Creator ID format")

    valid_creators_count = await db["creators"].count_documents({"_id": {"$in": creator_object_ids}})
    if valid_creators_count == 0:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="No valid creators provided")

    target_time = request.scheduled_for if request.scheduled_for else datetime.now(timezone.utc)
    
    job_operations = []
    job_ids = []
    
    # Grab priority from the parent campaign
    campaign_priority = campaign.get("priority", 0)
    
    for cid in creator_object_ids:
        # Cast ObjectId to string immediately so Pydantic's PyObjectId parser is happy
        job_id_str = str(ObjectId())
        job_ids.append(job_id_str)
        
        job = DispatchJobDocument(
            _id=job_id_str,
            campaign_id=campaign_id,
            creator_id=str(cid),
            scheduled_for=target_time,
            status=DispatchStatus.PENDING,
            priority=campaign_priority
        )
        job_operations.append(InsertOne(job.model_dump(by_alias=True)))

    if job_operations:
        inserted_count = 0
        try:
            result = await db["dispatch_jobs"].bulk_write(job_operations, ordered=False)
            inserted_count = result.inserted_count
        except BulkWriteError as bwe:
            # Captures the exact number of successful inserts before the duplicates failed
            inserted_count = bwe.details.get('nInserted', 0)
            ECHO.warning(f"Ignored {len(job_operations) - inserted_count} duplicate creators.")

        if inserted_count > 0:
            await db["campaigns"].update_one(
                {"_id": ObjectId(campaign_id)},
                {
                    "$inc": {"total_creators": inserted_count},
                    "$set": {"status": CampaignStatus.ACTIVE.value}
                }
            )

    delay_ms = 0
    if request.scheduled_for:
        now = datetime.now(timezone.utc)
        if request.scheduled_for > now:
            delay_ms = int((request.scheduled_for - now).total_seconds() * 1000)

    try:
        await RABBIT.publish_task(
            exchange_name="outreach.delayed",
            routing_key="campaign.dispatch",
            payload={
                "campaign_id": campaign_id,
                "job_ids": job_ids,
                "priority": campaign_priority
            },
            delay_ms=delay_ms
        )
        ECHO.info(f"Added {len(job_ids)} jobs to Campaign {campaign_id}. RMQ Delay: {delay_ms}ms.")
    except Exception as e:
        ECHO.error(f"Failed to publish to RMQ: {e}")

    return {
        "status": "success", 
        "campaign_id": campaign_id, 
        "jobs_staged": len(job_ids),
        "scheduled_for": target_time
    }