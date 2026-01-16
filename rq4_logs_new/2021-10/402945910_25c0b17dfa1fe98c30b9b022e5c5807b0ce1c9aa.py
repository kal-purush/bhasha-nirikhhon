from lostinp.models.user import User
from lostinp.utils.exceptions import BadRequest, Unauthorized


class LoginController:
    def __init__(self, auth_service, token_service, user_repo):
        self.auth_service = auth_service
        self.token_service = token_service
        self.user_repo = user_repo

    def do_it(self, request):
        """Main method to perform the whole login"""
        username, password = self._check(request)
        self._verify_user_exists(username, password)
        self._register_or_increment_count(username)
        token = self.token_service.create_user_token(username)
        return token

    def _verify_user_exists(self, username, password):
        """Check if the user is registered in the authentication service"""
        verfied = self.auth_service.verify(username, password)
        if not verfied:
            raise Unauthorized("No match for username and password")

    def _register_or_increment_count(self, username):
        """
        Check if this is the first login attempt to
        register into the database or increment the
        visit count
        """
        user = User({"username": username})
        registered = self.user_repo.is_user_registered(user)
        if registered:
            self.user_repo.increment_visit_counter(user)
        else:
            self.user_repo.register_user(user)

    def _check(self, request):
        """Verify all needed data is in the request"""
        try:
            username = request.get("username")
            User({"username": username}).validate()
            password = request["password"]
        except:
            raise BadRequest("Missing username or password in request")
        return username, password