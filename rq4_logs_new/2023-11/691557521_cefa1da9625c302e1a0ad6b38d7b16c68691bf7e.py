import unittest
from unittest.mock import AsyncMock, patch
from services.admins import AdminService


class TestAdminService(unittest.IsolatedAsyncioTestCase):
    @patch("db.admins.AdminDB")
    def setUp(self, mock_db):
        self.admin_service = AdminService(mock_db)
        self.mock_db = mock_db

    async def test_fetch_admins(self):
        mock_admins_data = [
            {
                "_id": "some-fake-id-1",
                "account": "adminuser",
                "password": "hashedpassword",
                "username": "Admin User",
                "role": "admin",
            },
            {
                "_id": "some-fake-id-2",
                "account": "secondadmin",
                "password": "anotherhashedpassword",
                "username": "Second Admin",
                "role": "moderator",
            },
        ]
        self.mock_db.read_admin = AsyncMock(return_value=mock_admins_data)
        admins = await self.admin_service.fetch_admins()
        self.assertEqual(admins, mock_admins_data)


if __name__ == "__main__":
    unittest.main()