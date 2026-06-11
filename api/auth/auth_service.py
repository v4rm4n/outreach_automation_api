# - outreach_automation_api/api/auth/auth_service.py -

from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import AUTHCFG
from services import ECHO, MONGO

from shared.models.user import UserDocument
from shared.security import verify_password

security = HTTPBearer()

class AuthService:
    def __init__(self):
        self.secret = AUTHCFG["JWT_SECRET"]
        self.algorithm = AUTHCFG["JWT_ALGORITHM"]
        self.expire_minutes = AUTHCFG["JWT_EXPIRE_MINUTES"]

    def create_token(self, user_id: str, role: str) -> str:
        payload = {
            "sub": user_id,
            "role": role,
            "iat": datetime.now(timezone.utc),
            "exp": datetime.now(timezone.utc) + timedelta(minutes=self.expire_minutes)
        }
        return jwt.encode(payload, self.secret, algorithm=self.algorithm)

    def verify_token(self, token: str) -> Optional[dict]:
        try:
            return jwt.decode(token, self.secret, algorithms=[self.algorithm])
        except jwt.ExpiredSignatureError:
            ECHO.warning("auth: token expired")
            return None
        except jwt.InvalidTokenError:
            ECHO.warning("auth: invalid token")
            return None

    async def authenticate_user(self, email: str, password: str) -> UserDocument | None:
        users = MONGO.get_collection("users")
        doc = await users.find_one({"email": email})
        if not doc:
            return None
        if not verify_password(password, doc["hashed_password"]):
            return None
        return UserDocument(**doc)

    async def get_current_user(
        self,
        credentials: HTTPAuthorizationCredentials = Depends(security)
    ) -> dict:
        payload = self.verify_token(credentials.credentials)
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
                headers={"WWW-Authenticate": "Bearer"}
            )
        return payload  # {"sub": user_id, "role": role}


auth_service = AuthService()