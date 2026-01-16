from datetime import datetime
from flask import current_app
from sqlalchemy.orm import validates
from App.database import db


class JobApplication(db.Model):
    """
    Represents a job application in the system.

    Attributes:
        id (int): A unique identifier for the job application.
        alumnus_id (int): Foreign key referencing the alumnus applying for the job.
        job_listing_id (int): Foreign key referencing the company hosting the job listing.
        datetime_applied (datetime): When the application was created.
        company_approval_status (str): Company approval status (e.g., "PENDING", "APPROVED").

    Relationships:
        alumni (relationship): Relationship to the 'AlumnusAccount' model.
        job_listings (relationship): Relationship to the 'JobListings' model.

    Note: See config file for valid approval statuses.
    """

    __tablename__ = "job_listings"

    id = db.Column(db.Integer(), primary_key=True)
    alumnus_id = db.Column(db.Integer, db.ForeignKey(
        'alumnus.id'), nullable=False)
    job_listing_id = db.Column(db.Integer, db.ForeignKey(
        'job_listing.id'), nullable=False)
    datetime_applied = db.Column(
        db.DateTime, nullable=False, default=datetime.now(datetime.timezone.utc))
    company_approval_status = db.Column(
        db.String(50), nullable=False, default="PENDING")

    alumni = db.relationship(
        "AlumnusAccount", back_populates="job_applications")
    job_listings = db.relationship(
        "JobListing", back_populates='job_applications')

    __table_args__ = (
        db.CheckConstraint(
            f"company_approval_status IN ({', '.join(repr(s) for s in current_app.config['APPROVAL_STATUSES'])})", name="valid_approval_status"),
    )

    def __init__(self, alumnus_id: int, job_listing_id: int) -> None:
        """
        Initializes a JobApplication instance.

        Args:
            alumnus_id (int): ID of the alumnus applying for the job.
            job_listing_id (int): ID of the job listing being applied to.
        """
        self.alumnus_id = alumnus_id
        self.job_listing_id = job_listing_id

    def __str__(self) -> str:
        """
        Returns a human-readable string representation of the job application.

        Returns:
            str: A formatted string displaying job application details.
        """
        return f"""{self.__class__.__name__} Info:
    - ID: {self.id}
    - Alumnus ID: {self.alumnus_id}
    - Job Listing ID: {self.job_listing_id}
    - Date/Time Applied: {self.datetime_applied.isoformat()}
    - Company Approval Status = {self.company_approval_status}
    """

    def __repr__(self) -> str:
        """
        Returns a developer-friendly representation of the job application.

        Returns:
            str: A string containing the job application details.
        """
        return (f"<{self.__class__.__name__} (id={self.id}, alumnus_id={self.alumnus_id}, "
                f"job_listing_id='{self.job_listing_id}'], "
                f"datetime_applied='{self.datetime_applied.isoformat()}, "
                f"company_approval_status='{self.company_approval_status}')>")

    def __json__(self):
        """
        Returns a JSON-serializable representation of the job application.

        Returns:
            dict: A dictionary containing job application details.
        """
        return {
            "id": {self.id},
            "alumnus_id": {self.company_id},
            "datetime_applied": {self.datetime_applied.isoformat()},
            "company_approval_status": {self.company_approval_status}
        }

    @staticmethod
    def get_valid_approval_statuses():
        """Get allowed approval statuses from the app config."""
        return current_app.config["APPROVAL_STATUSES"]

    @validates("approval_status")
    def validate_approval_status(self, value: str) -> str:
        """
        Ensures that 'approval_status' is valid.

        Args:
            key (str): The column being validated.
            value (str): The value assigned to 'approval_status'.

        Returns:
            str: Validated value.

        Raises:
            ValueError: If the status is invalid.
        """
        valid_statuses = self.get_valid_approval_statuses()
        if value not in valid_statuses:
            raise ValueError(
                f"Invalid status '{value}'. Allowed values: {valid_statuses}")
        return value