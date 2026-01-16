"use strict";

Object.defineProperty(exports, "__esModule", {
    value: true
});
exports.remove = exports.all = exports.add = undefined;

var _items = require("../entities/items");

var _items2 = _interopRequireDefault(_items);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

const add = exports.add = (req, res) => {

    const item = req.body.item;
    const category = req.body.category;

    const query = { item, category };

    let the_item = new _items2.default(query);

    the_item.save((err, data) => {
        if (!err) {
            console.log(data);
        }
    });
};

const all = exports.all = (req, res) => {
    const query = {};

    _items2.default.find(query, (err, items) => {
        if (!err) {
            res.json({ items });
        }
    });
};

const remove = exports.remove = (req, res) => {
    const query = req.params.id;
    _items2.default.findOneAndRemove(query, (err, item) => {
        if (!err) {
            res.json({ message: "item removed", item });
        }
    });
};

exports.default = {
    all, add, remove
};