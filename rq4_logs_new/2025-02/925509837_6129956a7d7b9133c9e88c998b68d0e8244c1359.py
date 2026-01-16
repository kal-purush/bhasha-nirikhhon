from App.database import db
from .base_user_account import BaseUserAccount


class CompanyAccount(BaseUserAccount):
    """
    Represents a company account in the system.

    This class extends `BaseUserAccount` to store additional company-specific 
    information, including contact details and job listings.

    Attributes:
        company_name (str): The name of the company (must be given and must be unique).
        phone_number (int): The company's phone number (must be given and must be unique).
        mailing_address (str): The company's mailing address (must be given, though it can be shared).
        website_url (str): The company's website URL (must be unique if given).
        notifications (relationship): Relationship to `Notification` model.
        job_listings (relationship): Relationship to `JobListing` model.
    """

    __tablename__ = "company_accounts"

    __mapper_args__ = {
        'polymorphic_identity': 'company',
    }

    company_name = db.Column(db.String, nullable=False, unique=True,)
    phone_number = db.Column(db.Integer, nullable=False, unique=True,)
    mailing_address = db.Column(db.String, nullable=False)
    website_url = db.Column(db.String, unique=True)

    notifications = db.relationship(
        'Notification', backref='company', lazy=True)
    job_listings = db.relationship('JobListing', backref='company', lazy=True)

    def __init__(self, email: str, password: str, company_name: str, phone_number: int, mailing_address: str, website_url: str) -> None:
        """
        Initializes a new `CompanyAccount` instance.

        Calls the `BaseUserAccount` constructor to set the email and password,
        then assigns company-specific attributes.

        Args:
            email (str): The company's email address.
            password (str): The company's plaintext password (hashed upon storage).
            company_name (str): The registered name of the company.
            phone_number (int): The company's contact number.
            mailing_address (str): The mailing address of the company.
            website_url (str): The official website URL of the company.
        """
        super().__init__(email, password)
        self.company_name = company_name
        self.phone_number = phone_number
        self.mailing_address = mailing_address
        self.website_url = website_url

    def __str__(self) -> str:
        """
        Returns a human-readable string representation of the company account.

        Returns:
            str: A formatted string displaying company account details (excluding password hash).
        """
        return f"""CompanyAccount Info:
    - ID: {self.id}
    - Email: {self.email}
    - Password Hash: [HIDDEN]
    - Company Name: {self.company_name}
    - Phone Number: {self.phone_number}
    - Mailing Address: {self.mailing_address}
    - Website URL: {self.website_url}
    """

    def __repr__(self) -> str:
        """
        Returns a developer-friendly representation of the company account.

        Returns:
            str: A string containing the company account details (excluding password hash).
        """
        return (f"<{self.__class__.__name__} (id={self.id}, email='{self.email}', "
                f"password_hash='[HIDDEN]', company_name='{self.company_name}', "
                f"phone_number={self.phone_number}, mailing_address='{self.mailing_address}', "
                f"website_url='{self.website_url}')>")

    def __json__(self) -> dict:
        """
        Returns a JSON-serializable representation of the company account.

        Returns:
            dict: A dictionary containing company account details (excluding password hash).
        """
        return {
            'id': self.id,
            'email': self.email,
            'password_hash': "[HIDDEN]",
            'company_name': self.company_name,
            'phone_number': self.phone_number,
            'mailing_address': self.mailing_address,
            'website_url': self.website_url
        }