import datetime

from django.db import models

from books.models import Book
from borrowings.notification.bot import create_message, send_message
from library import settings


def default_expected_date():
    date_of_borrowed = datetime.date.today()
    return date_of_borrowed + datetime.timedelta(days=7)


class Borrowing(models.Model):
    borrow_date = models.DateField(auto_now_add=True)
    expected_return_date = models.DateField(default=default_expected_date())
    actual_return_date = models.DateField(blank=True, null=True)
    book = models.ForeignKey(
        Book,
        on_delete=models.CASCADE,
        related_name="borrowings",
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="borrowings",
    )

    def __str__(self):
        return (
            f"{self.book.title} was borrow {self.borrow_date}"
            f" expected return {self.expected_return_date}"
        )

    def save(self, *args, **kwargs):
        message = create_message(self.expected_return_date,
                                 self.book.title,
                                 self.user)
        send_message(message)
        self.book.inventory -= 1
        self.book.save()
        super(Borrowing, self).save(*args, **kwargs)