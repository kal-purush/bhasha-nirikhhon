var size = 1;

while (size < 2048) {
  var result = new Array(size);
  var a = new Array(size);

  for (var i = 0; i < size; i++) {
    result[i] = new Array(size);

    for (var j = 0; j < size; j++) {
      result[i][j] = 0;
    }
  }
  for (var i = 0; i < size; i++) {
    a[i] = new Array(size);

    for (var j = 0; j < size; j++) {
      a[i][j] = Math.random();
    }
  }

  var start = new Date().getTime();

  for (var i = 0; i < a.length; i++) {
    for (var j = 0; j < a[0].length; j++) {
      // iterate through rows of Y
      for (var k = 0; k < a.length; k++) {
        result[i][j] += a[i][k] * a[k][j];
      }
    }
  }

  var end = new Date().getTime();

  console.log(Math.round(end - start));

  size *= 2;
}