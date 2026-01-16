'use strict';

/**
 * Module Dependencies
 */
var argv = require('yargs').argv;
var path = require('path');
var fs = require('fs-extra');
var debug = require('debug')('ironforger:'+path.basename(path.resolve(__filename)));
var inquirer = require('inquirer');
var replaceText = require('es-replace-text');

/**
 * Constants
 */
var ROOT_DIR = path.resolve(__dirname, '../');
var RECIPES_DIR = path.resolve(ROOT_DIR, 'recipes');
var CALLEE_DIR = path.resolve('.');

/**
 * Display Menu
 */
function menu() {
    inquirer.prompt(generatePrompts(), answerHandler);
}

/**
 * Generates the prompts for inquirer
 * a list of recipes
 * input for new name
 *
 * @returns {Array}
 */
function generatePrompts() {
    var prompts = [];

    prompts.push({
        type: 'list',
        name: 'recipe',
        message: 'Choose a recipe.',
        choices: getRecipes()
    });

    return prompts;
}

/**
 * copies the chosen recipe to callee's directory
 * renames the keyword to new name
 */
function answerHandler(answer) {
    var recipeDir = path.resolve(RECIPES_DIR, answer.recipe);
    var newName = answer.newName;

    copyDir(recipeDir, CALLEE_DIR);
}

/**
 * returns arr of recipe names
 */
function getRecipes() {
    var recipes = fs.readdirSync(RECIPES_DIR).filter(function(file) {
        if (/^\./.test(file)) return false;
        return file;
    });

    return recipes;
}

/**
 * copies source dir to target dir
 */
function copyDir(source, target) {
    var excludes = /(node_modules|bower_components)/;

    fs.readdirSync(source).forEach(function(file) {
        if (/^\./.test(file)) return;
        if (excludes.test(file)) return;
        console.log(file);
        fs.copySync(path.resolve(source, file), path.resolve(target, file));
    });
}

/**
 * Expose the menu function
 */
module.exports = menu;