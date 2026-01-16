/**
 * Represents a product.
 * @constructor
 * @param {string} id - The id of the product.
 * @param {string} name - The name of the product.
 * @param {string} price - The price of the product.
 * @example
 * //create product
 * var Cheese = New product(123, 'Dutch Cheese', 7.45);
 */
function Product(id, name, price) {
}

/**
 * Provides easy access to the system bus and provides some helper methods for doing so
 * @module mixins/bussable
 */
var bus = require( "postal" );
var Base = require( "../base" );
var sys = require( "lodash" );

