import json
import os
import requests
from datetime import datetime
from random import randint, random

from core.factories import (
    CommentFactory,
    CompanyFactory,
    CategoryFactory,
    ProductFactory,
    ReviewFactory,
    UserFactory,
)
from core.models import Category, ProductImage
from scripts.seeds.seed_categories import seed_categories
from utils import get_image_from_url

owners = UserFactory.create_batch(10)
users = [u for u in owners]

def seed_images (images, file_name, product):
    for index, url in enumerate(images, start=1):
        image = get_image_from_url(url, f"{file_name}_{index}")
        ProductImage.objects.get_or_create(product=product, image=image)

def seed_comments (reviews):
    for review in reviews:
        comUser = None
        if random() > 0.1:
            comUser = users[randint(0, len(users) - 1)]
        else:
            comUser = UserFactory()
        users.append(comUser)
        comments = CommentFactory.create_batch(
            randint(0, 5), review=review, user=comUser
        )

def seed_reviews (product):
    reviewUser = None
    if random() > 0.1:
        reviewUser = users[randint(0, len(users) - 1)]
    else:
        reviewUser = UserFactory()
        users.append(reviewUser)
    reviews = ReviewFactory.create_batch(
        randint(0, 30), product=product, user=reviewUser
    )
    seed_comments(reviews)

def seed_products (products, company):
    for prod in products:
        prod['company'] = company
        prod['category'] = Category.objects.get(name=prod['category'])
        if not 'quantity' in prod:
            prod['quantity'] = 1
        images = prod['images']
        del prod['images']
        newProduct = ProductFactory(**prod)
        seed_images(images, prod['title'], newProduct)
        seed_reviews(newProduct)

def seed_companies (companies):
    for company in companies:
        compObj = CompanyFactory(
            name=company['name'],
            owner=owners[randint(0, len(owners) - 1)]
        )
        seed_products(company['products'], compObj)

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
seed_dir = os.path.join(base_dir, "seeds/product_seed.json")
with open(seed_dir) as products_file:
    obj = json.load(products_file)
    seed_categories(obj['categories'])
    seed_companies(obj['companies'])
    