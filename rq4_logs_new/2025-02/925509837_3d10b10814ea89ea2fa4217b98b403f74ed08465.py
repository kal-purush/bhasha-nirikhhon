from App.database import db
from .base_user_account import BaseUserAccount


class AlumniAccount(BaseUserAccount):
    """
    Represents an alumni account, inheriting from the base user class.

    Alumni accounts store additional personal details, including name and phone number.
    """

    __tablename__ = "alumni_accounts"

    __mapper_args__ = {
        "polymorphic_identity": "alumni"
    }

    first_name = db.Column(db.String(50), nullable=False)
    last_name = db.Column(db.String(50), nullable=False)

    # String-type used to account for leading '0' and/or '+' in country codes, as well as separators '(e.g., '-') and extensions (e.g., "+1-(800)-555-1234 ext. 7890")
    # Phone numbers are not used for arithmetic, and their size can exceed that of int variables easily
    # Persons can share the same phone number (e.g., house number)
    phone_number = db.Column(db.String, nullable=True, unique=False)

    def __init__(self, email: str, password: str, first_name: str, last_name: str, phone_number: str = None) -> None:
        """
        Initializes a new Alumni account.

        Args:
            email (str): The alumni's email address (must be given and must be unique).
            password (str): The alumni's plaintext password (hashed internally before storage, must be given).
            first_name (str): The alumni's first name (must be given).
            last_name (str): The alumni's last name (must be given).
            phone_number (str, optional): The alumni's phone number (can be null). Example: "+1-(868)-123-4567 ext. 8910"
        """
        super().__init__(email, password)
        self.first_name = first_name
        self.last_name = last_name
        self.phone_number = phone_number

    def __str__(self) -> str:
        """
        Returns a human-readable string representation of the alumni account.

        Returns:
            str: A formatted string displaying alumni account details (excluding password hash).
        """
        return f"""{self.__class__.__name__} Info:
    - ID: {self.id}
    - Email: {self.email}
    - Password Hash: [HIDDEN]
    - First Name: {self.first_name}
    - Last Name: {self.last_name}
    - Phone Number: {self.phone_number if self.phone_number else '[N/A]'}
    """

    def __repr__(self) -> str:
        """
        Returns a developer-friendly representation of the alumni account.

        Returns:
            str: A string containing the alumni account details (excluding password hash).
        """
        return (f"<{self.__class__.__name__} (id={self.id}, email='{self.email}', "
                f"password_hash='[HIDDEN]', first_name='{self.first_name}', "
                f"last_name='{self.last_name}', "
                f"phone_number={self.phone_number if self.phone_number else '[N/A]'})>")

    def __json__(self) -> dict:
        """
        Returns a JSON-serializable representation of the alumni account.

        Returns:
            dict: A dictionary containing alumni account details (excluding password hash).
        """
        return {
            'id': self.id,
            'email': self.email,
            'password_hash': "[HIDDEN]",
            'first_name': self.first_name,
            'last_name': self.last_name,
            'phone_number': self.phone_number if self.phone_number else None
        }