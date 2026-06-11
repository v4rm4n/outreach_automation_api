# - outreach_automation_api/shared/models/campaign.py -

from datetime import datetime, timezone
from enum import Enum
from pydantic import BaseModel, Field
from shared.models.creator import PyObjectId

# --- Enums ---
class CampaignStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    COMPLETED = "completed"

class DispatchStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SENT = "sent"
    FAILED = "failed"

# --- Campaigns ---
class CampaignCreate(BaseModel):
    name: str
    template_id: str
    priority: int = Field(default=0, ge=0, le=10)

class AddCreatorsRequest(BaseModel):
    creator_ids: list[str]
    scheduled_for: datetime | None = None

class CampaignDocument(BaseModel):
    id: PyObjectId | None = Field(alias="_id", default=None)
    user_id: str
    template_id: str
    name: str
    status: CampaignStatus = CampaignStatus.DRAFT
    total_creators: int = 0
    priority: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    model_config = {"populate_by_name": True}

class CampaignResponse(BaseModel):
    id: PyObjectId = Field(alias="_id")
    user_id: str
    template_id: str
    name: str
    status: CampaignStatus
    total_creators: int
    priority: int
    created_at: datetime
    
    model_config = {"populate_by_name": True}

# --- Dispatch Jobs (Outbox) ---
class DispatchJobDocument(BaseModel):
    id: PyObjectId | None = Field(alias="_id", default=None)
    campaign_id: str
    creator_id: str
    status: DispatchStatus = DispatchStatus.PENDING
    priority: int = 0
    scheduled_for: datetime | None = None # The true source of scheduling!
    retry_count: int = 0
    error_message: str | None = None
    
    model_config = {"populate_by_name": True}