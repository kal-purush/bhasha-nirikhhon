import FS from 'fs';
import Minimist from 'minimist';

import ConfigService from './config/service.js';
import LoggerService from './logger/service.js';
import DatabaseService from './database/service.js';
import UnitsService from './units/service.js';

import IngredientsService from './ingredients/service.js';
import RecipeIngredientsService from './recipe-ingredients/service.js';
import RecipeTagsService from './recipe-tags/service.js';
import RecipesService from './recipes/service.js';
import TagsService from './tags/service.js';
import UsersService from './users/service.js';

(async function() {
	// initialize the services we will use
	await ConfigService.initialize();
	await LoggerService.initialize();
	await DatabaseService.initialize();
	await UnitsService.initialize();

	await UsersService.initialize();
	await RecipesService.initialize();
	await IngredientsService.initialize();
	await RecipeIngredientsService.initialize();

	// parse arguments
	const argv = Minimist(process.argv.slice(2));
	const file_location = argv.file;
	const user_id = argv.user;

	// open up the file and parse it
	var recipes = null;
	try {
		const file = FS.readFileSync(file_location);
		recipes = JSON.parse(file);
	} catch(err) {
		LoggerService.red(`File is not valid`);
		process.exit();
	}

	// check that the user provided is a real user
	const user = await UsersService.getById(user_id);
	if(user == null)
	{
		LoggerService.red(`User is not valid`);
		process.exit();
	}

	// loop through all the recipes and do stuff with them
	var i = 1;
	for(const recipe of recipes)
	{
		LoggerService.yellow(`Recipe (${ i } / ${ recipes.length })`);
		await createRecipe(recipe, user);

		i ++;
	}

	process.exit();
})();

async function createRecipe(recipe_data, creator)
{
	const recipe = await RecipesService.create({
		creator_id: creator.id,
		published: false,
		instructions: recipe_data.instructions,
		name: recipe_data.name,
		description: recipe_data.description,
	});

	if(recipe == null)
		LoggerService.red(`Failed to create Recipe "${ recipe.name }"`);
	else
	{
		for(const r_i of recipe_data.ingredients)
		{
			const r_i_data = await parseIngredient(r_i);

			if(r_i_data == null)
				LoggerService.red(`Failed to create Ingredient "${ r_i }"`);
			else
			{
				var recipe_ingredient = await RecipeIngredientsService.create({
					recipe_id: recipe.id,
					ingredient_id: r_i_data.ingredient_id,
					quantity: r_i_data.amount,
					units: r_i_data.units,
				});

				if(recipe_ingredient == null)
					LoggerService.red(`Failed to create RecipeIngredient "${ r_i }"`);
				else
					LoggerService.green(`Created RecipeIngredient "${ r_i }"`);
			}
		}

		LoggerService.green(`Created Recipe: "${ recipe.name }"`);
	}
}

async function parseIngredient(recipe_ingredient)
{
	var front, amount, units, unit_type, ingredient_name;
	try {
		front = recipe_ingredient.match(/[0-9]+\s[A-Za-z]+/)[0];
		amount = front.split(' ')[0];
		units = front.split(' ')[1];
		unit_type = UnitsService.getTypeOfUnit(units);
		ingredient_name = recipe_ingredient.substr(front.length + 1, recipe_ingredient.length);
	} catch(err) {
		return null;
	}
	if(unit_type == null)
		return null;

	var ingredient = await IngredientsService.getByName(ingredient_name);
	if(ingredient == null)
	{
		ingredient = await IngredientsService.create({
			name: ingredient_name,
			description: 'An ingredient',
			unit_type: unit_type,
		});
	}

	return {
		amount: amount,
		units: units,
		ingredient_id: ingredient.id,
	};
}