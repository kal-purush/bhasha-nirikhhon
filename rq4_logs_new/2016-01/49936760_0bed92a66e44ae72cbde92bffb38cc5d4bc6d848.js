/**
* this is for testing
* creat by ivan jiao
* This module contains classes for running a store.
* @module Store
*/



var Store = Store || {};

/**
* 'TAX_RATE' is stored as a percentage. Value is 13.
    * @property TAX_RATE
    * @static
    * @final
    * @type Number
*/
 
Store.TAX_RATE = 13;


/**	
 * 一些示例代码：
 
	var options = {
		paths: [ './lib' ],
		outdir: './out'
	};

	var Y = require('yuidoc');
	var json = (new Y.YUIDoc(options)).run();
	
 * @class Item 
 * @constructor
 * @param name {String} 条目名称
 * @param price {Number} 条目价格
 * @param quantity {Number} Item quantity (the number available to buy)
 */

 
Store.Item = function (name, price, quantity) {
    /**
     * @property name
     * @type String
     */
    this.name = name;
    /**
     * @property price
     * @type String
     */
    this.price = price * 100;
    /**
     * @property quantity
     * @type Number
     */
    this.quantity = quantity;
    /**
     * @property id
     * @type Number
     */
    this.id = Store.Item._id++;
    Store.Item.list[this.id] = this;
};


/**
 * `_id` is incremented when a new item is created, so every item has a unique ID
 * @property id
 * @type Number
 * @static
 * @private
 */
Store.Item._id = 1;
 
/**
 * @property list
 * @static
 * @type Object
 */
Store.Item.list = {};


/**
 * @class Cart
 * @constructor
 * @param name {String} 顾客姓名
 */
 
Store.Cart = function (name) {
    /**
     * @property name
     * @type String
     */
    this.name = name;
    /**
     * @property items
     * @type Object
     * @default {}
     */
    this.items = {};
};


/**
 * @method total
 * @return {Number} tax-included total value of cart contents
 */
 
Store.Cart.prototype.total = function () {
    var subtotal, id;
    subtotal = 0;
    for (id in this.items) {
        if(this.items.hasOwnProperty(id)) {
            subtotal += Store.Item.list[id].price * this.items[id];
        }
    }
    return parseFloat(((subtotal * (1 + Store.TAX_RATE / 100)) / 100).toFixed(2));
};