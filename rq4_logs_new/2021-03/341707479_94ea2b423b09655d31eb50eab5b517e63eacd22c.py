from products.models import Product


def product_dict(inventory_product) -> dict:
    product = Product.objects.get(id=inventory_product.id)

    id = product.id
    name = product.name
    category = product.category
    image = product.image
    price = product.price
    description = product.description

    product_data = dict(
        id=id,
        name=name,
        category=category,
        image=image,
        price=price,
        description=description
    )

    return product_data