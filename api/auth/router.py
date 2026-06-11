# - outreach_automation_api/api/auth/router.py -

from fastapi import APIRouter, HTTPException, status
from pymongo.errors import DuplicateKeyError

from services import ECHO, MONGO
from shared.models.user import UserCreate, UserDocument, UserResponse, LoginRequest, LoginResponse
from shared.security import hash_password
from .auth_service import auth_service

router = APIRouter(
    prefix="/auth",
    tags=["Authentication"]
)

@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(request: UserCreate):
    users = MONGO.get_collection("users")
    
    doc = UserDocument(
        email=request.email,
        full_name=request.full_name,
        hashed_password=hash_password(request.password)
    )

    try:
        # Rely on database level unique constraint atomicity
        result = await users.insert_one(doc.model_dump(by_alias=True, exclude={"id"}))
    except DuplicateKeyError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, 
            detail="Email already registered"
        )
    
    ECHO.debug(f"Inserted user with id: {result.inserted_id}")
    
    return UserResponse(
        _id=str(result.inserted_id),
        email=doc.email,
        full_name=doc.full_name,
        role=doc.role,
        is_active=doc.is_active,
        created_at=doc.created_at
    )

@router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    user = await auth_service.authenticate_user(request.email, request.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail="Invalid credentials"
        )
    
    token = auth_service.create_token(str(user.id), user.role.value)
    return LoginResponse(
        access_token=token,
        user=user.to_response()
    )