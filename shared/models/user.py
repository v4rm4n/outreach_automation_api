# - outreach_automation_api/shared/models/user.py -

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, EmailStr, Field
from pydantic.functional_validators import BeforeValidator

PyObjectId = Annotated[str, BeforeValidator(str)]

class UserRole(str, Enum):
    ADMIN = "admin"
    USER = "user"

# Base - shared fields
class UserBase(BaseModel):
    email: EmailStr
    full_name: str

# For registration request
class UserCreate(UserBase):
    password: str

# For login request
class LoginRequest(BaseModel):
    email: EmailStr
    password: str

# What lives in MongoDB
class UserDocument(UserBase):
    id: PyObjectId | None = Field(alias="_id", default=None)
    hashed_password: str
    role: UserRole = UserRole.USER
    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

    def to_response(self) -> "UserResponse":
        assert self.id is not None, "UserDocument.id must be set before calling to_response()"
        return UserResponse(
            _id=self.id,
            email=self.email,
            full_name=self.full_name,
            role=self.role,
            is_active=self.is_active,
            created_at=self.created_at
        )

# What API returns - never expose hashed_password
class UserResponse(UserBase):
    id: PyObjectId = Field(alias="_id")
    role: UserRole
    is_active: bool
    created_at: datetime

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

# Login response
class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse