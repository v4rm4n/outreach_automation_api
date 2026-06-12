# --- outreach_automation_api/integrations/instagram/client.py ---

import asyncio
from typing import Any
from instagrapi import Client
from instagrapi.exceptions import UserNotFound, RateLimitError, ChallengeRequired
from config import APPCFG
from services import ECHO

# Your burner account credentials from .env
IG_USERNAME = APPCFG.get("IG_USERNAME", "YOUR_BURNER_USERNAME")
IG_PASSWORD = APPCFG.get("IG_PASSWORD", "YOUR_BURNER_PASSWORD")

class InstagramClient:
    """
    Async wrapper for the instagrapi Client to send real DMs.
    Bypasses official Graph API restrictions using mobile emulation.
    """

    def __init__(self, max_attempts: int = 2):
        self.max_attempts = max_attempts
        self.cl = Client()
        self.is_logged_in = False
        
        if not IG_USERNAME or IG_USERNAME == "YOUR_BURNER_USERNAME":
            ECHO.warning("No Instagram burner credentials found. API calls will fail.")

    def _sync_login(self):
        """Synchronous login block to be executed in a separate thread."""
        if not self.is_logged_in:
            ECHO.info(f"Logging into burner account @{IG_USERNAME}...")
            self.cl.login(IG_USERNAME, IG_PASSWORD)
            self.is_logged_in = True
            ECHO.info("Login successful!")

    def _sync_send(self, handle: str, text: str) -> dict:
        """Synchronous block to fetch user ID and send the DM."""
        # 1. Convert string handle (e.g., 'glossy_priya') to numerical IG User ID
        user_id = self.cl.user_id_from_username(handle)
        
        # 2. Send the message
        result = self.cl.direct_send(text, [int(user_id)])
        
        # Return a success dictionary
        return {"status": "success", "thread_id": getattr(result, 'thread_id', 'unknown')}

    async def send_direct_message(self, handle: str, text: str, **kwargs: Any) -> dict:
        """
        Sends a real DM to the specified Instagram handle.
        """
        loop = asyncio.get_running_loop()
        last_exception = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                # 1. Ensure we are logged in (runs once per worker lifecycle)
                await loop.run_in_executor(None, self._sync_login)
                
                # 2. Execute the send operation
                result = await loop.run_in_executor(None, self._sync_send, handle, text)
                
                ECHO.debug(f"Successfully sent real DM to @{handle}")
                return result

            except ChallengeRequired as e:
                # Instagram flagged the account and wants phone/email verification
                ECHO.error(f"Account flagged! Challenge Required for @{IG_USERNAME}")
                self.is_logged_in = False  # Force re-login next time if fixed manually
                raise RuntimeError("Instagram Account Challenge Required (Burner Burned)") from e

            except UserNotFound as e:
                # User doesn't exist or blocked you
                ECHO.error(f"User @{handle} not found or account is private/deleted.")
                raise ValueError(f"Cannot message user @{handle}: User Not Found") from e

            except RateLimitError as e:
                # Action blocked because you sent too many DMs too quickly
                ECHO.warning(f"Rate limit hit on burner account! Attempt {attempt}/{self.max_attempts}")
                last_exception = e
                if attempt < self.max_attempts: 
                    await asyncio.sleep(min(2 ** attempt, 10))
                continue

            except Exception as e:
                last_exception = e
                ECHO.warning(f"Network/Library attempt {attempt} failed: {e}")
                if attempt < self.max_attempts: 
                    await asyncio.sleep(min(2 ** attempt, 10))

        ECHO.error(f"All dispatch attempts failed for @{handle}")
        raise last_exception or RuntimeError("instagrapi dispatch failed")

INSTAGRAM = InstagramClient()