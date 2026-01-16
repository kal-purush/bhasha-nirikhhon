const gcd = function(u, v) {
    return (v != 0) ? gcd(v, u % v) : u;
}
module.exports = gcd; //TODO: export on ES6