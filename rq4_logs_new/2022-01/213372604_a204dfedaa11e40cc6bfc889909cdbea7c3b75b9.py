from django.core.management import BaseCommand

from events.models import Enrolment


class Command(BaseCommand):
    help = "Sends notifications asking for feedback about a recently held event."

    def handle(self, *args, **options):
        self.stdout.write("Sending feedback notifications...")

        count = Enrolment.objects.send_feedback_notifications()

        self.stdout.write(f"Sent {count} occurrence feedback notification(s).")