from app.models import db, Note, environment, SCHEMA
from sqlalchemy.sql import text

def seed_notes():
    recipe1 = Note(
        user_id=3, notebook_id=4, title='Mint Chocolate Brownies',
        content="""
        <strong>Ingredients:</strong>
        <ul>
            <li>10 ounces dairy free dark chocolate</li>
            <li>1 cup of Earth Balance butter</li>
            <li>4 organic happy free-range eggs</li>
            <li>2 cup packed light brown sugar</li>
            <li>10 oz almond meal</li>
            <li>½ cup sweet white rice flour</li>
            <li>1 teaspoon fine sea salt</li>
            <li>½ teaspoon baking soda</li>
            <li>½ teaspoon xanthan gum</li>
            <li>4 teaspoons vanilla extract</li>
            <li>1 ½ peppermint extract</li>
            <li>1 bar of dairy free mint dark chocolate, chopped</li>
        </ul>
        """, trash=False
    )
    recipe2 = Note(
        user_id=3, notebook_id=4, title="Sweet Potato Pie",
        content="""
        <strong>Ingredients:</strong>
        <ul>
            <li>⅓ cup butter, softened</li>
            <li>½ cup sugar</li>
            <li>2 Eggland's Best Eggs, lightly beaten</li>
            <li>¾ cup evaporated milk</li>
            <li>2 cups mashed sweet potatoes</li>
            <li>1 teaspoon vanilla extract</li>
            <li>½ teaspoon ground cinnamon</li>
            <li>½ teaspoon ground nutmeg</li>
            <li>¼ teaspoon salt</li>
            <li>1 unbaked pastry shell (9 inches)</li>
        </ul>
        """, trash=False
    )
    food_ideas = Note(
        user_id=3, notebook_id=4, title="Sweet Treats",
        content="""

        """, trash=False
    )
    bad_ideas = Note(
        user_id=3, notebook_id=4, title="Really Good Ideas",
        content="""

        """, trash=True
    )


def undo_notes():
    pass