# - outreach_automation_api/api/creators/router.py -

import csv
import io
from fastapi import UploadFile, File, APIRouter, Depends, HTTPException, status
from pymongo import UpdateOne, ReturnDocument

from api.auth.auth_service import auth_service
from services import MONGO
from shared.models.creator import CreatorCreate, CreatorDocument, CreatorResponse, BulkUploadResponse

router = APIRouter(
    prefix="/creators",
    tags=["Creators"],
    dependencies=[Depends(auth_service.get_current_user)]
)

@router.post("", response_model=CreatorResponse, status_code=status.HTTP_201_CREATED)
async def add_creator(request: CreatorCreate):
    creators = MONGO.get_collection("creators")

    # Only include fields that are explicitly set and non-null
    update_data = {
        k: v for k, v in request.model_dump(exclude_unset=True).items()
        if v is not None
    }


    doc_data = CreatorDocument(**request.model_dump()).model_dump(
        by_alias=True, exclude={"id"}
    )

    updated_doc = await creators.find_one_and_update(
        {"handle": request.handle, "platform": request.platform},
        [
            {
                "$set": {
                    **{k: {"$ifNull": [f"${k}", v]} for k, v in update_data.items()},
                }
            },
            {
                "$set": {
                    "created_at": {"$ifNull": ["$created_at", doc_data["created_at"]]}
                }
            }
        ],
        upsert=True,
        return_document=ReturnDocument.AFTER
    )

    return CreatorResponse(**updated_doc)

@router.post("/bulk", response_model=BulkUploadResponse)
async def bulk_upload(file: UploadFile = File(...)):
    creators = MONGO.get_collection("creators")
    
    inserted = 0
    skipped = 0
    errors = []
    raw: list[dict] = []
    
    content = await file.read()

    # 1. Parse File Content safely (CSV Only)
    # Browsers sometimes send CSVs as octet-stream or ms-excel MIME types
    if file.content_type in ("text/csv", "application/octet-stream", "application/vnd.ms-excel"):
        try:
            text_data = content.decode("utf-8")
            reader = csv.DictReader(io.StringIO(text_data))
            raw = [row for row in reader]
        except Exception as e:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"CSV parse error: {e}")
    else:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Only CSV format is supported")

    if not raw:
        return BulkUploadResponse(inserted=0, skipped=0, errors=["File contains no data"])

    # 2. Stage operations for Bulk Writing
    operations = []
    for i, row in enumerate(raw):
        try:
            # Handle array flattening from CSV: convert "tag1, tag2" back to ["tag1", "tag2"]
            if "tags" in row and isinstance(row["tags"], str):
                row["tags"] = [tag.strip() for tag in row["tags"].split(",") if tag.strip()]
            elif "tags" not in row:
                row["tags"] = []

            creator = CreatorCreate(**row)
            doc_data = CreatorDocument(**creator.model_dump()).model_dump(by_alias=True, exclude={"id"})
            
            # Use Mongo Upsert rules over a network single batch
            operations.append(
                UpdateOne(
                    {"handle": creator.handle, "platform": creator.platform},
                    {"$setOnInsert": doc_data},
                    upsert=True
                )
            )
        except Exception as e:
            errors.append(f"Row {i + 1} Validation Failure: {e}")
            continue

    # 3. Execute batch network transaction
    if operations:
        try:
            # ordered=False allows operations to continue even if individual mutations throw errors
            result = await creators.bulk_write(operations, ordered=False)
            
            inserted = result.upserted_count
            skipped = result.matched_count
        except Exception as e:
            errors.append(f"Database batch execution failure: {e}")

    return BulkUploadResponse(inserted=inserted, skipped=skipped, errors=errors)