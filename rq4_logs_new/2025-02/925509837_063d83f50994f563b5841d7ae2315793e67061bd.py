from App.database import db


class CompanySubscription(db.Model):
    """
    Represents a alumnus' subscription to a company to recieve updates whenever the company posts new job listings.

    Attributes:
        alumnus_id (int): Foreign key referencing the alumnus creating the subscription.
        company_id (int): Foreign key referencing the company the alumnus is subscribing to.

        alumnus (relationship): Many-to-one relationship to the 'AlumnusAccount' model.
        company (relationship): Many-to-one relationship to the 'CompanyAccount' model.
    """

    __tablename__ = "company_subscriptions"

    alumnus_id = db.Column(db.Integer(), db.ForeignKey(
        'alumnus.id'), primary_key=True, nullable=False)
    company_id = db.Column(db.Integer(), db.ForeignKey(
        'company.id'), primary_key=True, nullable=False)

    alumni = db.relationship(
        "AlumnusAccount", back_populates="comany_subscriptions")
    companies = db.relationship(
        "CompanyAccount", back_populates='subscribed_alumni')

    def __init__(self, alumnus_id: int, company_id: int) -> None:
        """
        Initializes a CompanySubscription instance.

        Args:
            alumnus_id (int): ID of the alumnus creating the subscription.
            company_id (int): ID of the company the alumnus is subscribing to.
        """
        self.alumnus_id = alumnus_id
        self.company_id = company_id

    def __str__(self) -> str:
        """
        Returns a human-readable string representation of the company subscription.

        Returns:
            str: A formatted string displaying company subscription details.
        """
        return f"""{self.__class__.__name__} Info:
    - Alumnus ID: {self.alumnus_id}
    - Company ID: {self.alumnus_id}
    """

    def __repr__(self) -> str:
        """
        Returns a developer-friendly representation of the job application.

        Returns:
            str: A string containing the job application details.
        """
        return (f"<{self.__class__.__name__} (alumnus_id={self.alumnus_id}, company_id={self.company_id})>")

    def __json__(self):
        """
        Returns a JSON-serializable representation of the job application.

        Returns:
            dict: A dictionary containing job application details.
        """
        return {
            "alumnus_id": {self.company_id},
            "company_id": {self.company_id}
        }