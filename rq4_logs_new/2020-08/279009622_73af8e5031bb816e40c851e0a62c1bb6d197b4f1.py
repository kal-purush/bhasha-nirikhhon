import random
from core.factories import (
    ProductFactory, ProductLineFactory, ReviewFactory, CommentFactory,
    UserFactory, CompanyFactory, CategoryFactory
)

category_names = ['Video Games', 'Clothing', 'Movies & TV']
categories = []
for name in category_names:
    categories.append(CategoryFactory(name=name))
owners = UserFactory.create_batch(10)
users = [u for u in owners]

def seed_comments (reviews):
    for review in reviews:
        comUser = None
        if random.random() > 0.1:
            comUser = users[
                random.randint(0, len(users) - 1)]
        else:
            comUser = UserFactory()
        users.append(comUser)
        comments = CommentFactory.create_batch(
            random.randint(0, 3), review=review, user=comUser
        )

def seed_reviews (product_lines):
    for line in product_lines:
        reviewUser = None
        if random.random() > 0.1:
            reviewUser = users[random.randint(0, len(users) - 1)]
        else:
            reviewUser = UserFactory()
            users.append(reviewUser)
        reviews = ReviewFactory.create_batch(
            random.randint(0, 10), product=line, user=reviewUser
        )
        seed_comments(reviews)

def seed_products (product_lines):
    for line in product_lines:
        products = ProductFactory.create_batch(
            random.randint(0, 5), product_line=line
        )

def seed_product_lines (companies):
    for company in companies:
        category = categories[random.randint(0, len(categories) - 1)]
        product_lines = ProductLineFactory.create_batch(
            random.randint(0, 5), company=company, category=category
        )
        seed_products(product_lines)
        seed_reviews(product_lines)

def seed_companies (owners):
    for owner in owners:
        companies = CompanyFactory.create_batch(
            random.randint(0, 2), owner=owner
        )
        seed_product_lines(companies)


seed_companies(owners)