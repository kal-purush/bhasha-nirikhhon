from datetime import datetime
from flask import current_app
from sqlalchemy.orm import validates
from App.database import db


class JobListing(db.Model):
    """
    Represents a job listing in the system.

    Attributes:
        id (int): A unique identifier for the job listing.
        company_id (str): Foreign key referencing the company posting the job.
        title (str): The job title.
        position_type (str): The type of employment (e.g., full-time, contract).
        description (str): Job requirements and details.
        monthly_salary_ttd: Salary in Trinidad and Tobago Dollars.
        is_remote (bool): Indicates if the job it remote.
        job_site (str): Physical job location (set to "N/A" if remote).
        datetime_created (datetime): When the job listing was created.
        datetime_last_modified (datetime): When the job listing was last modified.
        admin_approval_status (str): Admin approval status (e.g., "PENDING", "APPROVED").

    Relationships:
        creator_company (relationship): Relationship to the 'CompanyAccount' model.
        job_applications (relationship): Relationship to the 'JobApplication' model.

    Note: See config file for valid position types and approval statuses.
    """

    __tablename__ = "job_listings"

    id = db.Column(db.Integer(), primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey(
        'company.id'), nullable=False)
    title = db.Column(db.String(120), nullable=False)
    position_type = db.Column(db.String(50), nullable=False)
    description = db.Column(db.String(1000), nullable=False)
    monthly_salary_ttd = db.Column(db.Integer, nullable=False)
    is_remote = db.Column(db.Boolean, nullable=False, default=False)
    job_site = db.Column(db.String(120), nullable=False)
    datetime_created = db.Column(
        db.DateTime, nullable=False, default=datetime.now(datetime.timezone.utc))
    datetime_last_modified = db.Column(db.DateTime, nullable=False, default=datetime.now(
        datetime.timezone.utc), onupdate=datetime.now(datetime.timezone.utc))
    admin_approval_status = db.Column(
        db.String(50), nullable=False, default="PENDING")

    company = db.relationship("CompanyAccount", back_populates="job_listings")
    job_applications = db.relationship(
        "JobApplication", back_populates='job_listing', cascade="all, delete-orphan")

    __table_args__ = (
        db.CheckConstraint(
            f"position_type IN ({', '.join(repr(s) for s in current_app.config['JOB_POSITION_TYPES'])})", name="valid_position_type"),
        db.CheckConstraint(
            f"admin_approval_status IN ({', '.join(repr(s) for s in current_app.config['APPROVAL_STATUSES'])})", name="valid_approval_status"),
    )

    def __init__(self, company_id, title, position_type, description, monthly_salary_ttd, is_remote, job_site):
        """
        Initializes a JobListing instance.

        Args:
            company_id (int): ID of the company posting the job.
            title (str): Job title.
            position_type (str): Type of employment (e.g., Full-Time, Contract).
            description (str): Job description.
            salary (int): Salary in TTD.
            is_remote (bool): Whether the job is remote.
            job_site (str): Job location (set to "N/A" if remote).
        """
        self.company_id = company_id
        self.title = title
        self.position_type = position_type
        self.description = description
        self.monthly_salary_ttd = monthly_salary_ttd
        self.is_remote = is_remote
        self.job_site = job_site if not is_remote else "N/A"

    def __str__(self) -> str:
        """
        Returns a human-readable string representation of the job listing.

        Returns:
            str: A formatted string displaying job listing details.
        """
        return f"""{self.__class__.__name__} Info:
    - ID: {self.id}
    - Company ID: {self.company_id}
    - Job Title: {self.title}
    - Position Type: {self.position_type}
    - Job Description: {self.description}
    - Monthly Salary (TTD): {self.monthly_salary_ttd}
    - Is Remote: {self.is_remote}
    - Job Site: {self.job_site}
    - Date/Time Created: {self.datetime_created.isoformat()}
    - Date/Time Last Modified: {self.datetime_last_modified.isoformat()}
    - Admin Approval Status = {self.admin_approval_status}
    """

    def __repr__(self) -> str:
        """
        Returns a developer-friendly representation of the job listing.

        Returns:
            str: A string containing the job listing details.
        """
        return (f"<{self.__class__.__name__} (id={self.id}, company_id={self.company_id}, "
                f"title='{self.title}'], position_type='{self.position_type}', "
                f"description='{self.description}', monthly_salary_ttd={self.monthly_salary_ttd}, "
                f"is_remote={self.is_remote}, job_site='{self.job_site}', "
                f"datetime_created='{self.datetime_created.isoformat()}', "
                f"datetime_last_modified='{self.datetime_last_modified.isoformat()}, "
                f"admin_approval_status='{self.admin_approval_status}')>")

    def __json__(self):
        """
        Returns a JSON-serializable representation of the job listing.

        Returns:
            dict: A dictionary containing job listing details.
        """
        return {
            "id": {self.id},
            "company_id": {self.company_id},
            "title": {self.title},
            "position_type": {self.position_type},
            "description": {self.description},
            "monthly_salary_ttd": {self.monthly_salary_ttd},
            "is_remote": {self.is_remote},
            "job_site": {self.job_site},
            "datetime_created": {self.datetime_created.isoformat()},
            "datetime_last_modified": {self.datetime_last_modified.isoformat()},
            "admin_approval_status": {self.admin_approval_status}
        }

    @staticmethod
    def get_valid_approval_statuses():
        """Get allowed approval statuses from the app config."""
        return current_app.config["APPROVAL_STATUSES"]

    @staticmethod
    def get_valid_job_position_types():
        """Get allowed job position types from the app config."""
        return current_app.config["JOB_POSITION_TYPES"]

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

    @validates("position_type")
    def validate_position_type(self, value: str) -> str:
        """
        Ensures that 'position_type' is valid.

        Args:
            key (str): The column being validated.
            value (str): The value assigned to 'position_type'.

        Returns:
            str: Validated value.

        Raises:
            ValueError: If the position type is invalid.
        """
        valid_types = self.get_valid_job_position_types()
        if value not in valid_types:
            raise ValueError(
                f"Invalid position type '{value}'. Allowed values: {valid_types}")
        return value

    # def notify_observers(self, alumni, company):
    #     """Notify the company (observer) about an alumni applying."""
    #     if company:
    #         # Create a notification message
    #         message = f"Alumni {alumni.username} applied to your listing '{self.title}'."

    #         # Save the notification in the database
    #         notification = Notification(message=message, company_id=company.id, listing_id=self.id)
    #         db.session.add(notification)
    #         db.session.commit()

    # def get_notifications(self):
    #     return Notification.query.filter_by(listing_id=self.id).all()