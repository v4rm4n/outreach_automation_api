# - outreach_automation_api/api/templates/router.py -

from fastapi import APIRouter, Depends, status
from api.auth.auth_service import auth_service, TokenPayload
from services import MONGO
from shared.models.template import *

router = APIRouter(
    prefix="/templates",
    tags=["Templates"],
    dependencies=[Depends(auth_service.get_current_user)]
)

@router.post("", response_model=TemplateResponse, status_code=status.HTTP_201_CREATED)
async def create_template(
    request: TemplateCreate,
    user: TokenPayload = Depends(auth_service.get_current_user)
):
    templates = MONGO.get_collection("templates")
    doc = TemplateDocument(user_id=user.sub, **request.model_dump())
    
    result = await templates.insert_one(doc.model_dump(by_alias=True, exclude={"id"}))
    
    return TemplateResponse(_id=str(result.inserted_id), **doc.model_dump(exclude={"id"}))