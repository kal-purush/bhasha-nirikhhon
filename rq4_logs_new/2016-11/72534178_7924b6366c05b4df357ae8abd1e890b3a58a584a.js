var NativeFloat32Array = typeof(Float32Array) !== "undefined" ? Float32Array : Array,
    AttributePrototype;


module.exports = Attribute;


function Attribute() {
    this.name = null;
    this.array = null;
    this.itemSize = null;
    this.dynamic = null;
}
AttributePrototype = Attribute.prototype;

Attribute.create = function(name, length, itemSize, ArrayType, dynamic, items) {
    return (new Attribute()).construct(name, length, itemSize, ArrayType, dynamic, items);
};

AttributePrototype.construct = function(name, length, itemSize, ArrayType, dynamic, items) {

    ArrayType = ArrayType || NativeFloat32Array;

    this.name = name;
    this.array = new ArrayType(length);
    this.itemSize = itemSize;
    this.dynamic = !!dynamic;

    if (items) {
        this.array.set(items);
    }

    return this;
};

AttributePrototype.destructor = function() {

    this.name = null;
    this.array = null;
    this.itemSize = null;
    this.dynamic = null;

    return this;
};

AttributePrototype.setDynamic = function(value) {
    if (this.dynamic === value) {
        return this;
    }

    this.dynamic = value;
    return this;
};

AttributePrototype.size = function() {
    return this.array.length / this.itemSize;
};

AttributePrototype.set = function(array) {

    this.array.set(array);
    return this;
};

AttributePrototype.setX = function(index, x) {

    this.array[index * this.itemSize] = x;
    return this;
};

AttributePrototype.setY = function(index, y) {

    this.array[index * this.itemSize + 1] = y;
    return this;
};

AttributePrototype.setZ = function(index, z) {

    this.array[index * this.itemSize + 2] = z;
    return this;
};

AttributePrototype.setXY = function(index, x, y) {
    var array = this.array;

    index = index * this.itemSize;
    array[index] = x;
    array[index + 1] = y;

    return this;
};

AttributePrototype.setXYZ = function(index, x, y, z) {
    var array = this.array;

    index = index * this.itemSize;
    array[index] = x;
    array[index + 1] = y;
    array[index + 2] = z;

    return this;
};

AttributePrototype.setXYZW = function(index, x, y, z, w) {
    var array = this.array;

    index = index * this.itemSize;
    array[index] = x;
    array[index + 1] = y;
    array[index + 2] = z;
    array[index + 3] = w;

    return this;
};