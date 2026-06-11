# - outreach_automation_api/api/auth/router.py -

from fastapi import APIRouter, HTTPException

from services import ECHO, MONGO
from shared.models.user import *
from shared.security import hash_password

from .auth_service import auth_service

api_router = APIRouter(
    prefix="/auth",
    tags=["Authentication"]
)

@api_router.post("/register", response_model=UserResponse)
async def register(request: UserCreate):
    users = MONGO.get_collection("users")
    
    existing = await users.find_one({"email": request.email})
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")
    
    doc = UserDocument(
        email=request.email,
        full_name=request.full_name,
        hashed_password=hash_password(request.password)
    )

    result = await users.insert_one(doc.model_dump(by_alias=True, exclude={"id"}))
    
    ECHO.debug(f"Inserted user with id: {result.inserted_id}")
    return UserResponse(
        _id=str(result.inserted_id),
        email=doc.email,
        full_name=doc.full_name,
        role=doc.role,
        is_active=doc.is_active,
        created_at=doc.created_at
    )

@api_router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    user = await auth_service.authenticate_user(request.email, request.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    token = auth_service.create_token(str(user.id), user.role.value)
    return LoginResponse(
        access_token=token,
        user=user.to_response()
    )