import unittest
# from app.modules.resources.admin import AdminManagement
from app.modules.resources.admin import AdminManagement


class AdminResourceTestCase(unittest.TestCase):
    def login(self, email, password):
        am = AdminManagement()
        return am.login_admin(email, password)

    def register(self, email, password, first_name, last_name):
        am = AdminManagement()
        return am.create_admin(first_name, last_name, email, password)

    def find(self, email):
        am = AdminManagement()
        return am.find_admin(email)

    def test_register(self):
        self.register("burrito@email.com", "password", "Bu", "rrito")
        rv = self.find("burrito@email.com")

        self.assertEqual(
            rv, {
                "last_name": "rrito",
                "first_name": "Bu",
                "email": "burrito@email.com",
                "password": "password"
            })