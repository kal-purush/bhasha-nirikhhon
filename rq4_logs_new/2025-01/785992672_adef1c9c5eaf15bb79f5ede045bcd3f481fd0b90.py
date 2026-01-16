from typing import List
import httpx
import logging
from datetime import datetime
import socket
from app.models.port_forward import PortForward
from app.utils.config import settings

logger = logging.getLogger(__name__)

class SupabaseManager:
    def __init__(self):
        self.base_url = f"{settings.SUPABASE_URL}/rest/v1"
        self.headers = {
            "apikey": settings.SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {settings.SUPABASE_SERVICE_ROLE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }
        self.hostname = socket.gethostname()

    async def update_port_forwards(self, port_forwards: List[PortForward]) -> None:
        """Update port forwards in Supabase database"""
        try:
            async with httpx.AsyncClient() as client:
                # First, mark all existing records for this host as inactive
                update_url = f"{self.base_url}/port_forwards"
                update_params = {
                    "host_name": f"eq.{self.hostname}"
                }
                await client.patch(
                    update_url,
                    headers=self.headers,
                    params=update_params,
                    json={"is_active": False}
                )

                logger.debug(f"Marked old records as inactive for host: {self.hostname}")

                # Convert port forwards to Supabase format
                records = []
                for pf in port_forwards:
                    # Set the hostname
                    pf.host_name = self.hostname
                    # Convert to Supabase format
                    records.append(pf.to_supabase())

                # Insert new records
                if records:
                    insert_url = f"{self.base_url}/port_forwards"
                    response = await client.post(
                        insert_url,
                        headers=self.headers,
                        json=records
                    )

                    if response.status_code in (200, 201, 204):
                        logger.info(f"Successfully updated {len(records)} port forwards in Supabase")
                    else:
                        logger.error(f"Error updating Supabase: {response.status_code} - {response.text}")
                else:
                    logger.warning("No port forwards to update")

        except Exception as e:
            logger.error(f"Error updating Supabase: {e}")
            raise