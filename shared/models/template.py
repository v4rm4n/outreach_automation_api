# - outreach_automation_api/shared/models/template.py -

from datetime import datetime, timezone
from pydantic import BaseModel, Field
from shared.models.creator import PyObjectId

class TemplateCreate(BaseModel):
    name: str
    body: str

class TemplateDocument(TemplateCreate):
    id: PyObjectId | None = Field(alias="_id", default=None)
    user_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    model_config = {"populate_by_name": True}

class TemplateResponse(TemplateCreate):
    id: PyObjectId = Field(alias="_id")
    user_id: str
    created_at: datetime
    
    model_config = {"populate_by_name": True}