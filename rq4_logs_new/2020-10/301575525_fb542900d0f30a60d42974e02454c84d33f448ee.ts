const sqx = (p: any) => {
    var width = document.querySelector('body').clientWidth;
    var height = document.querySelector('body').clientHeight;

    const margin_denominator = 4;

    var marginx = width / margin_denominator;
    var marginy = height / margin_denominator;

    var rand_rect_size = p.random() * 100;

    var x = width * (margin_denominator - 2) / margin_denominator - rand_rect_size;
    var y = height * (margin_denominator - 2) / margin_denominator - rand_rect_size;

    var randx = p.random() * x;
    var randy = p.random() * y;

    p.setup = function () {
      p.createCanvas(width, height);
      p.background(0);
    };
    p.draw = function () {
      p.fill(255);
      randx = p.random() * x;
      randy = p.random() * y;
      p.rect(randx + marginx, randy + marginy, rand_rect_size, rand_rect_size);
    };
  };
  let squares = document.createElement('div');
  new p5(sqx, squares);
  window.document.getElementsByTagName('body')[0].appendChild(squares);