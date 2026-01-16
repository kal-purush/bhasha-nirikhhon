var x = 1;
var y = 2;
var c1 = new Coordinate(x, y);

if (c1.x() != x)
  throw new Error("x coordinate doesn't match");
if (c1.y() != y)
  throw new Error("y coordinate doesn't match");
if (!c1.equal(new Coordinate(x, y)))
  throw new Error("equal coordinate aren't equal()");
if (c1.equal(new Coordinate(x + 1, y + 1)))
  throw new Error("unequal coordinate are equal()");

    