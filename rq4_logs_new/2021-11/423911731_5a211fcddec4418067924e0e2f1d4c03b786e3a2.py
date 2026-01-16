import random

from django.core.management import BaseCommand
from faker import Faker

from core.models import Ingredient, Recipe, User, Tag

fakegen = Faker()


def create_super_user():
    user = User.objects.create_superuser(email="attachemd@gmail.com", password="1234")
    user.save()
    return user


def create_super_user2():
    user = User.objects.get_or_create(email="attachemd@gmail.com", password="1234")
    user.is_admin = True
    user.is_superuser = True
    user.is_staff = True
    user.save()
    return user


def populate_user(n=5):
    for _ in range(n):
        name = fakegen.name()
        first_name = name.split(' ')[0]
        last_name = ' '.join(name.split(' ')[-1:])
        username = first_name[0].lower() + last_name.lower().replace(' ', '')
        email = username + "@" + last_name.lower() + ".com"
        user = User.objects.create_user(email, password=username)
        user.name = username
        user.is_superuser = False
        user.is_staff = False
        user.save()


def populate_data():
    user = create_super_user()
    for _ in range(20):
        # create fake data for entry
        fake_name = fakegen.word()

        # create new movie entry
        tag = Tag.objects.get_or_create(
            user=user,
            name=fake_name
        )[0]
        # tags.append(tag)

    for _ in range(20):
        # create fake data for entry
        fake_name = fakegen.word()

        # create new movie entry
        ingredient = Ingredient.objects.get_or_create(
            user=user,
            name=fake_name
        )[0]

    for _ in range(10):
        recipe_name = fakegen.text(max_nb_chars=30)

        recipe = Recipe.objects.create(
            user=user,
            # tags=[tagid],
            # ingredients=[ingredientid],
            title=recipe_name,
            time_minutes=random.randint(1, 60),
            price=round(random.uniform(1.00, 100.00), 2),
            link=fakegen.url(),
        )

        tag_ids = random.sample(range(1, 20), random.randint(1, 10))
        ingredient_ids = random.sample(range(1, 20), random.randint(1, 10))
        ingredients = Ingredient.objects.filter(
            id__in=ingredient_ids
        )
        tags = Tag.objects.filter(
            id__in=tag_ids
        )
        recipe.tags.set(tags)
        recipe.ingredients.set(ingredients)


class Command(BaseCommand):
    help = "Command information"

    def handle(self, *args, **kwargs):
        print('populating script')
        # create_super_user()
        # populate_tag(20)
        populate_user()
        populate_data()
        # populate_ingredient(20)
        print('populating complete')