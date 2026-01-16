from django.core.management.base import BaseCommand
from teachers.models import Teacher
from faker import Faker


class Command(BaseCommand):
    help = 'Generates 100 random teachers'

    def handle(self, *args, **options):
        fake = Faker()
        for i in range(100):
            fake_teacher = {
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'age': fake.random_int(min=18, max=100)
            }
            teacher = Teacher(**fake_teacher)
            teacher.save()

        self.stdout.write(self.style.SUCCESS('Created 100 teachers'))