/// <reference path = "Coordinate.ts"/>
/// <reference path = "utils.ts"/>
/// <reference path = "Graph.ts"/>
/// <reference path = "raphael.d.ts"/>


module LogAnimator {

  export class
  T {

    // Draw a log animation within a rectangle.

    constructor(
	paper		// where to draw the rectangle.
      , ht		// the rectangle's height
      , wd		// the rectangle's width.
      , ul: Coordinate	// The rectangle's upper-left corner.
      , g: Graph	// The graph involved.
      , log		// The log to animate.
      ) {

      this.nextStep = 0;

      var fontSize = Math.round(ht*0.8);
      var msgFontSize = Math.round(fontSize*0.5);

      this.history = this.fillHistory(log);

      var nameWd = this.findNameWd(paper, this.history, fontSize);
      var gap = 5;

      this.poppedRep = paper.text(
	ul.x() + gap + nameWd/2, ul.y() + ht*0.5, "").attr({
	  stroke: foregroundColor
	, fill: foregroundColor
	, 'font-size': fontSize
	, 'text-anchor': "middle"
	});
      drawLine(paper,
	ul.x() + 2*gap + nameWd, ul.y() + ht*0.1,
	ul.x() + 2*gap + nameWd, ul.y() + ht*0.9)
      this.qRep = paper.text(ul.x() + nameWd + 3*gap, ul.y() + ht*0.5, "").attr({
	  stroke: foregroundColor
	, fill: foregroundColor
	, 'font-size': fontSize
	, 'text-anchor': "start"
	});
      this.msgRep = paper.text(ul.x() + wd - gap, ul.y() + ht*0.5, "")
	.attr({
	  stroke: foregroundColor
	, fill: foregroundColor
	, 'font-size': msgFontSize
	, 'text-anchor': "end"
	})
      }


    private
    drawQ(step) {

      var qStr = ""
      var sep = ""

      for (var i = 0; i < step.q.length; ++i) {
	qStr = qStr + sep + step.q[i]
	sep = "  "
	}

      this.qRep.attr({ text: qStr });
      this.poppedRep.attr({ text: step.lastPopped });
      this.msgRep.attr({ text: step.msg });

      console.log('drew "' + this.qRep.attr('text') + '".');
      }


    private
    fillHistory(log): any [] {

      var q = [];
      var lastPopped = "";
      var msg = "";
      var history = [];

      for (var i = 0; i < log.length; ++i) {
	var step = log[i];

	if (step[0] == 'clear') {
	  q = [];
	  lastPopped = "";
	  msg = "";
	  }

	else if (step[0] == 'deq')
	  lastPopped = q.shift();

	else if (step[0] == 'enq')
	  q.push(step[1]);

	else if (step[0] == 'msg')
	  msg = step[1];

	else if (step[0] == 'pop')
	  lastPopped = q.shift();

	else if (step[0] == 'push')
	  q.unshift(step[1]);

	else if (step[0] == 'swap') {
	  var t = q[step[1]];
	  q[step[1]] = q[step[2]];
	  q[step[2]] = t;
	  }

	else
	  throw new Error('unrecognized operator in log: ' + step[0]);

	history.push({
	    q: q.slice()
	  , lastPopped: lastPopped
	  , msg: msg
	  });
	}

      return history;
      }


    private
    findNameWd(paper, log, fs: number) {

      var nwd = -1;
      var scratch = paper.text(-100, -100, 100, 100, "").attr('font-size', fs);

      setIterate(log, function (n) {
	scratch.attr('text', n.lastPopped);
	var bb = scratch.getBBox();
	nwd = Math.max(bb.width, nwd);
	});

      scratch.remove();

      return nwd;
      }


    step() {
      this.drawQ(this.history[this.nextStep]);
      this.nextStep = (this.nextStep + 1) % this.history.length;
      }


    private nextStep: number; 
    private history: any [];

    private qRep;
    private poppedRep;
    private msgRep;
    }
  }