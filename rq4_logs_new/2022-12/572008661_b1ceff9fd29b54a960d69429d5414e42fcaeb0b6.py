import datetime
from borrowings.models import Borrowing
from borrowings.notification.bot import send_message


def create_daily_message(return_date, borrow_date, book_name, user_email):
    return (f"Borrowing overdue: \n"
            f"Borrow date:{borrow_date} \n"
            f"Expected return date:{return_date} \n"
            f"Book: {book_name} \n"
            f"User: {user_email}")


def daily_message():
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    queryset = Borrowing.objects.all().filter(
        actual_return_date=None,
        expected_return_date__lte=tomorrow
    )
    if queryset:
        for borriwing in queryset:
            message = create_daily_message(borriwing.expected_return_date, borriwing.borrow_date, borriwing.book.title, borriwing.user)
            send_message(message)
    else:
        send_message("No borrowings overdue today!")