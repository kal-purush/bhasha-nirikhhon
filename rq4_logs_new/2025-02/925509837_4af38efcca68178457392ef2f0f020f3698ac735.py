from werkzeug.security import check_password_hash, generate_password_hash
from App.database import db


class BaseUserAccount(db.Model):
    """
    Base class for user accounts, providing authentication features.

    This class is abstract and should be inherited by specific user types.
    It supports password hashing and verification.
    """

    __abstract__ = True

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), nullable=False, unique=True)
    password_hash = db.Column(db.String(120), nullable=False)
    type = db.Column(db.String(50))

    __mapper_args__ = {
        'polymorphic_identity': 'user',
        'polymorphic_on': type
    }

    def __init__(self, email: str, password: str) -> None:
        """
        Initialize a new BaseUserAccount.

        Args:
            email (str): The user's email address.
            password (str): The user's plaintext password (will be hashed).
        """
        self.set_password(password)
        self.email = email

    def __str__(self) -> str:
        """
        Returns a human-readable string representation of the user account.

        Returns:
            str: A formatted string with user details (excluding password hash).
        """
        return f"""BaseUserAccount Info:
    - ID: {self.id}
    - Email: {self.email}
    - Password Hash: [HIDDEN]"""

    def __repr__(self) -> str:
        """
        Returns a developer-friendly representation of the user account.

        Returns:
            str: A string containing the class name, ID, and email.
        """
        return f"<{self.__class__.__name__} (id={self.id}, email='{self.email}', password_hash='[HIDDEN]')>"

    def __json__(self) -> dict:
        """
        Returns a JSON-serializable representation of the user account.

        Returns:
            dict: A dictionary containing user details (excluding password hash).
        """
        return {
            'id': self.id,
            'email': self.email,
            'password_hash': "[HIDDEN]"
        }

    @property
    def password(self) -> None:
        """
        Prevent direct access to the password attribute.

        Raises:
            AttributeError: Always raises an error to prevent access.
        """
        raise AttributeError(
            "Password is not accessible directly. Use set_password() or check_password() instead.")

    @password.setter
    def password(self, password: str) -> None:
        """
        Sets a new password for the user (hashing it before storing).

        Args:
            password (str): The plaintext password.
        """
        self.set_password(password)

    def set_password(self, password: str):
        """
        Hashes and stores the user's password securely.

        Args:
            password (str): The plaintext password.
        """
        self.password_hash = generate_password_hash(password, method='sha256')

    def check_password(self, password: str) -> bool:
        """
        Verifies if the provided password matches the stored hashed password.

        Args:
            password (str): The plaintext password to verify.

        Returns:
            bool: True if the password matches, False otherwise.
        """
        return check_password_hash(self.password_hash, password)