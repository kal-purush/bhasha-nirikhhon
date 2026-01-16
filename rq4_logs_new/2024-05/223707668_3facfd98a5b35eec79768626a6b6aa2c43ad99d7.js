/**
 * ini_parse_quantity
 *
 * 思ったよりややこしかったので超簡易実装（どうせ kmg しか使わん）。
 */
module.exports = function ini_parse_quantity(quantity) {
    quantity = ('' + quantity).toLowerCase();
    var unit = quantity.slice(-1);
    var size = parseInt(quantity || 0);
    if (unit === 'k') {
        size *= 1024;
    }
    if (unit === 'm') {
        size *= 1024 * 1024;
    }
    if (unit === 'g') {
        size *= 1024 * 1024 * 1024;
    }
    return size;
};