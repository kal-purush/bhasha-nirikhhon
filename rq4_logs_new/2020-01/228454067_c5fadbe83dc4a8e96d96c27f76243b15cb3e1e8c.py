from entities.error_response import ErrorResponse
from entities.ingredient_entity import Ingredient
from entities.ingredient_recipe import IngredientRecipe
from entities.messages import Message
from entities.recipe_entity import Recipe
from entities.response import Response
from errors.recipe_errors import recipe_errors
from errors.recipe_errors import (RecipeDoesNotExist,
                                  NotCorrectFormatError,
                                  RecipeCreationSuccess)
from facades.recipe_facade import recipe_facade
from logs import Logger
from models.ingredients_model import IngredientsModel
from models.ingredients_recipes_model import IngredientsRecipesModel
from models.recipe_model import RecipeModel

# TODO:
# Error 401: La petición viene sin el token de sesión
# Error 403: El token ha caducado


def recipes_creation(recipe_request):
    logger = Logger('recipe_creation::controller::flask')
    logger.debug('Creating recipe')
    try:

        recipe = Recipe(**recipe_request)

        if not recipe_facade.recipe_creation_validation(recipe):
            raise NotCorrectFormatError

        for related_recipe in recipe.recipes_related:
            if not RecipeModel.find_by_name(related_recipe):
                raise RecipeDoesNotExist

        for ingredient in recipe.ingredients:
            if not ingredient in IngredientsModel.find_by_name(ingredient):
                ingredient = IngredientsModel(recipe.name)
                ingredient.save()

        recipe = RecipeModel(**recipe_request)
        recipe.save()

        for ingredient in recipe.ingredients:
            ingredient_recipe = IngredientsRecipesModel(
                ingredient.quantity, recipe.id, ingredient)
            ingredient_recipe.save()

        return Response(
            recipe_errors['RecipeCreationSuccess']['status'],
            Message(recipe_errors['RecipeCreationSuccess']['message']).toJson()
        )

    except NotCorrectFormatError:
        error_response = ErrorResponse('NotCorrectFormatError', recipe)
        logger.warning('The recipe has not a correct format')
        return Response(error_response.code, error_response.toJson())
    except Exception:
        error_response = ErrorResponse('CreatingRecipeError', recipe)
        logger.error('Database error')
        return Response(error_response.code, error_response.toJson())

        # 1. Comprobar si los datos de la receta son validos (hay que crear un facade)
        #   1.1 El nombre de receta debe de estar informado
        #   1.2 Debe de tener al menos un igrediente
        #   1.3 Los pasos de la receta deben de estar informados
        #   1.4 La foto debe de estar informada
        # 2. Hay que comprobar la coherencia de las relaciones en base de datos
        #   2.1 Si la receta tiene recetas relacionadas hay que comprobar que dichas recetas existen ya
        # 3. Hay que dar de alta los ingredientes
        #   3.1 Comprobar si los ingredientes existen en base de datos y si no existen crearlos
        # 4. Hay que salvar la receta en base de datos
        # 5. Hay que salvar la relación entre los ingredientes y la receta mediante el modelo
        #    IngredientsRecipesModel añadiendo la cantidad de cada ingrediente