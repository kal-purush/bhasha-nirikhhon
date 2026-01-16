function Plate(weight, amount) {
    this.weight = weight;
    this.amount = amount;
}

Plate.prototype.clone = function() {
    return new Plate(this.weight, this.amount);
};