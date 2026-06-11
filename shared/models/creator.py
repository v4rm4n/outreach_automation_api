# - outreach_automation_api/shared/models/creator.py -

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, Field
from pydantic.functional_validators import BeforeValidator

PyObjectId = Annotated[str, BeforeValidator(str)]

class Platform(str, Enum):
    INSTAGRAM = "instagram"
    YOUTUBE = "youtube"
    TIKTOK = "tiktok"
    TWITTER = "twitter"

# MongoDB Doc
class CreatorDocument(BaseModel):
    id: PyObjectId | None = Field(alias="_id", default=None)
    handle: str                        # @glossy_priya
    platform: Platform
    email: str | None = None           # for email outreach
    full_name: str | None = None
    niche: str | None = None           # skincare, fitness, etc
    followers: int | None = None
    region: str | None = None          # IN, US, etc
    language: str = "en"
    tags: list[str] = Field(default_factory=list)
    profile_url: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

# For single JSON entry from UI
class CreatorCreate(BaseModel):
    handle: str
    platform: Platform
    email: str | None = None
    full_name: str | None = None
    niche: str | None = None
    followers: int | None = None
    region: str | None = None
    language: str = "en"
    tags: list[str] = Field(default_factory=list)
    profile_url: str | None = None

# API returns
class CreatorResponse(BaseModel):
    id: PyObjectId = Field(alias="_id")
    handle: str
    platform: Platform
    email: str | None = None
    full_name: str | None = None
    niche: str | None = None
    followers: int | None = None
    region: str | None = None
    language: str = "en"                         
    tags: list[str] = Field(default_factory=list)
    profile_url: str | None = None
    created_at: datetime

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

# Bulk upload response
class BulkUploadResponse(BaseModel):
    inserted: int
    skipped: int
    errors: list[str] = Field(default_factory=list)