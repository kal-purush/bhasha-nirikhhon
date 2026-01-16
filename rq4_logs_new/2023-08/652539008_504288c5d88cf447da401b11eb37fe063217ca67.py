import os
import random
import uuid
from faker import Faker
from django.core.management.base import BaseCommand

# Set the DJANGO_SETTINGS_MODULE environment variable to your settings file
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'qarbar.settings')

import django
django.setup()

from property.models import (
    Property,
    Area, 
    Agent, 
    PropertyAmenties, 
    PropertyTypes, 
    PropertyLocation, 
    PropertyInstallment, 
    Media
)
from django.contrib.auth.models import User
from django.db.models.signals import post_save
from django.dispatch import receiver
from users.models import UserProfile
from django.utils.timezone import timezone
from django.utils import timezone


class Command(BaseCommand):
    help = 'Add test items in store'

    @receiver(post_save, sender=User)
    def create_profile(sender, instance, created, **kwargs):
        if created and not UserProfile.objects.filter(user=instance):
            UserProfile.objects.get_or_create(user=instance)

    def generate_dummy_data(self):
        fake = Faker()
        counter = 1
        for _ in range(100):
            # Generate a unique username for the dummy user
            username = f"dummy_user_{uuid.uuid4().hex[:8]}"
            # Create a dummy user
            dummy_user, _ = User.objects.get_or_create(username=username, defaults={'password': 'dummy_password'})

            # Create Area, Agent, PropertyAmenities, PropertyTypes, PropertyLocation, and PropertyInstallment instances
            area = Area.objects.create(area=fake.city())
            agent = Agent.objects.create(
                user=dummy_user,
                name=fake.name(),
                email=fake.email(),
                phone_number=fake.phone_number(),
                bio=fake.paragraph(),
                nationality=fake.country(),
                languages=fake.language_code(),
                areas=area.area,
                experience_since=fake.date_between(start_date='-10y', end_date='today')
            )
            amenties, created = PropertyAmenties.objects.get_or_create(
                bedrooms=random.randint(1, 5),
                bathrooms=random.randint(1, 3),
                gym=fake.boolean(),
                swimming_pool=fake.boolean(),
                balcony=fake.boolean(),
                other_nearby_palces=fake.sentence(),
                distance_from_airport=random.randint(1, 20),
                built_in_year=random.randint(1900, timezone.now().year),
                kitchen=random.randint(1, 3),
                floors=random.randint(1, 10),
                maid_room=fake.boolean(),
                built_in_wardrobes=fake.boolean(),
                kitchen_appliances=fake.boolean(),
                lower_portion=fake.boolean(),
                Farmhouse=fake.boolean(),
                electricity_backup=fake.boolean(),
                furnished_unfurnished=fake.boolean(),
                covered_parking=fake.boolean(),
                lobby_in_building=fake.boolean(),
                security=fake.boolean(),
                parking_space=fake.boolean(),
                drawing_room=fake.boolean(),
                study_room=fake.boolean(),
                laundry_room=fake.boolean(),
                store_room=fake.boolean(),
                lounge_sitting_area=fake.boolean(),
                internet=fake.boolean(),
                mosque=fake.boolean(),
                kids_play_area=fake.boolean(),
                medical_center = fake.boolean(),
                community_lawn_garden = fake.boolean(),
                near_by_school = fake.boolean(),
                near_by_hospital = fake.boolean(),
                near_by_shopping_mall =fake.boolean(), 
                other_description = fake.sentence(),
                # Add other amenities fields as needed
            )
            property_type = PropertyTypes.objects.create(
                home_types=random.choice(['house', 'flat', 'room', 'pent_house']),
                # Add other property types data as needed
            )
            property_location = PropertyLocation.objects.create(
                latitude=fake.latitude(),
                longitude=fake.longitude()
            )
            installment = PropertyInstallment.objects.create(
                advance_amount=random.randint(10000, 50000),
                no_of_inst=random.randint(6, 24),
                monthly_inst=random.randint(5000, 10000),
                ready_for_possession=fake.boolean()
            )

            # Create Property instance
            property_instance = Property.objects.create(
                title=fake.sentence(),
                phone=fake.phone_number(),
                landline=fake.phone_number(),
                secondry_phone=fake.phone_number(),
                email=fake.email(),
                rent_sale_type=random.choice(['rent', 'sale']),
                area=area,
                agent=agent,
                amenties=amenties,
                property_type=property_type,
                property_location=property_location,
                installment=installment,
                available=fake.boolean(),
                description=fake.paragraph(),
                total_price=random.randint(100000, 1000000),
                date=fake.date_between(start_date='-1y', end_date='today')
            )

            # Create Media instances associated with the property
            for _ in range(random.randint(5, 10)):
                media_type = random.choice(['image', 'Video'])
                image_url = fake.image_url(width=None, height=None)
                Media.objects.create(property=property_instance, media_type=media_type, image_url=image_url)

    if __name__ == "__main__":
        generate_dummy_data()
    
    def handle(self, *args, **options):
        self.generate_dummy_data()
