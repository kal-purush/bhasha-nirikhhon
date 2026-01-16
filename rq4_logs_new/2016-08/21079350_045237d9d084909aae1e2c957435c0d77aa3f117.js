
// var repdraw;

// function initialize_circles() {
//     var canv = document.getElementById("mycanvas");
//     var cont = canv.getContext("2d");
//     cont.clearRect(0, 0, canv.width, canv.height);
//     cont.strokeRect(0, 0, canv.width, canv.height);
//     draw = function() {
// 	x = Math.random() * canv.width;
// 	y = Math.random() * canv.height;
// 	cont.beginPath();
// 	cont.arc(x, y, Math.random() * 10,
// 		 0, Math.PI * 2, true);
// 	cont.closePath();
// 	cont.strokeStyle = "#ff0000";
// 	cont.fillStyle = "#330000";
// 	cont.fill();
// 	cont.stroke();
//     }
//     if (repdraw) clearInterval(repdraw);
//     repdraw = setInterval(draw, 200);
// }


//Create the renderer
var renderer =
    new PIXI.CanvasRenderer(
    // PIXI.autoDetectRenderer(
	1000, 800,
	{ antialias: false, transparent: true, resolution: 1 }
    );

//Create a container object called the `stage`
var stage = new PIXI.Container();

var stages = new Object();

function initPIXI()
{
    //Add the canvas to the HTML document
    document.body.appendChild(renderer.view);
    // set a border?
    renderer.view.style.border = "1px solid red";
    // misc stuff
    renderer.autoResize = true; // for any future resize
    //Tell the `renderer` to `render` the `stage`
    renderer.render(stage);
}

function initPage()
{
    initPIXI();
    stages[0] = stage; // for multiple layers later
}


// basic primitives

gprim = {
    // c refers to the 'context'
    clear : function(c, fill) {
	context.clearRect(0, 0, context.canvas.width, context.canvas.height);
	if (fill) {
	    context.fillStyle = fill;
	    context.fillRect(0, 0, context.canvas.width, context.canvas.height);
	}
    },

    setStroke : function(context, stroke) {
	context.strokeStyle = stroke;
    },
    setFill : function(context, fill) {
	if (fill === true) context.fillStyle = context.strokeStyle;
	else if (fill !== false) context.fillStyle = fill;
	/ ignore fill === false
    },
    setWidth : function(context, lwd) { // not used yet
	context.lineWidth = lwd;
    },
    setFont : function(context, family, fill, ps, cex) {
	context.font = ps*cex + "ps " + family;
	context.fillStyle = fill;
	context.textAlign = "center";
	context.textBaseline = "middle";
    },

    circleFilled : function(context, x, y, r)
    {
	context.beginPath();
	context.arc(x, y, 5 * r, 0, Math.PI * 2, true);
	context.closePath();
	context.fill();
	context.stroke();
    },

    circleEmpty : function(context, x, y, r)
    {
	context.beginPath();
	context.arc(x, y, 5 * r, 0, Math.PI * 2, true);
	context.closePath();
	context.fill(); 
	context.stroke();
    },

    linesV : function(context, x, y)
    {
	// vectorized so that a single context.stroke() will do
	var i, n = x.length;
	context.beginPath();
	context.moveTo(x[0], y[0]);
	for (i = 1; i < n; i++)
	{
	    context.lineTo(x[i], y[i]);
	}
	context.stroke();
    },

    segmentsV : function(context, x1, y1, x2, y2)
    {
	// vectorized so that a single context.stroke() will do
	var i, n = x1.length;
	context.beginPath();
	for (i = 0; i < x1.length; i++)
	{
	    context.moveTo(x1[i], y1[i]);
	    context.lineTo(x2[i], y2[i]);
	}
	context.stroke();
    },

    polygonFilled : function(context, x, y) {
	var i, n = x.length;
	context.beginPath();
	context.moveTo(x[0], y[0]);
	for (i = 1; i < n; i++)
	{
	    context.lineTo(x[i], y[i]);
	}
	context.lineTo(x[0], y[0]);
	context.fill();
	context.stroke();
    },
    polygonEmpty : function(context, x, y) {
	var i, n = x.length;
	context.beginPath();
	context.moveTo(x[0], y[0]);
	for (i = 1; i < n; i++)
	{
	    context.lineTo(x[i], y[i]);
	}
	context.lineTo(x[0], y[0]);
	context.stroke();
    },

    rectangleFilled : function(context, x0, y0, x1, y1)
    {
	context.fillRect(x0, y0, x1 - x0, y1 - y0);
	context.strokeRect(x0, y0, x1 - x0, y1 - y0);
    },

    rectangleEmpty : function(context, x0, y0, x1, y1)
    {
	context.strokeRect(x0, y0, x1 - x0, y1 - y0);
    },

    strwidth : function(context, str) 
    {
	return context.measureText(str).width;
    },

    text1 : function(context, x, y, str, rot) 
    {
	// always middle-aligned. Need to do placement computations elsewhere (currently R)
	context.save();
	context.translate(x, y);
	if (rot != 0) context.rotate(-rot * 0.01745329) // (PI * 180);
	context.fillText(labels, 0, 0);
	context.restore();
    },


    // // font, cex, ps, col, gamma
    // if (col) {
    //     c.translate(x, y);
    //     if (rot != 0) c.rotate(-rot * 0.01745329); /* =pi/180 (rot : degrees -> radian) */
    //     c.fillStyle = col;
    //     c.textAlign = "start";
    //     c.textBaseline = "bottom";
    //     var fontwt = "normal";
    //     if (fontface == 2) fontwt = "bold";
    //     else if (fontface == 3) fontwt = "italic";
    //     else if (fontface == 4) fontwt = "bold italic";
    //     // c.font =  "italic 12px "Unknown Font", sans-serif"
    //     var ofont = c.font;
    //     c.font = fontwt + " " + Math.round(cex*ps) + "px " + fontfamily;
    //     c.fillText(str, -hadj * c.measureText(str).width, 0);
    //     c.font = ofont;
    //     c.setTransform(1, 0, 0, 1, 0, 0); // reset transformation
    // }

    clipRect : function(context, xleft, ybottom, xright, ytop)
    {
	context.beginPath();
	context.moveTo(xleft, ybottom);
	context.lineTo(xleft, ytop);
	context.lineTo(xright, ytop);
	context.lineTo(xright, ybottom);
	context.closePath();
	context.clip();
    }
    
}


// Define API for R to talk to canvas

// For starters, keep colors etc non-vectorized, and specified
// separately as theme-like settings. That is, parameters should be
// specified in separate calls. Also, coordinates in calls made from R
// should be integers (i.e., conversion should be done in R).

targets = [ stage ];

// canvas uses string colors, and alpha as part of it, unlike PIXI

var theme = {
    target : 0, // stage to target. May have multiple for layered drawing.
    stroke : "#000000",
    fill : false,
    family : "sans-serif",
    ps : 10;
    cex : 1, // multiply with ps
    // more: bold, italic, ?
};

function setPar(s, val)
{
    theme[s] = val;
}

function points(x, y, r)
{
    var i, n = x.length, c = stages[theme['target']];
    var circleFun;
    if (!r) r = [5];
    with(gprim)
    {
	setStroke(c, theme['stroke']);
	setFill(c, theme['fill']);
	if (theme['fill'] !== false) circleFun = circleEmpty;
	else circleFun = circleFilled;
	if (r.length === n) {
	    for (i = 0; i < n; i++) circleFun(c, x[i], y[i], r[i]);
	}
	else {
	    for (i = 0; i < n; i++) circleFun(c, x[i], y[i], r[0]);
	}
    }
}

function lines(x, y)
{
    var i, n = x.length, c = stages[theme['target']];
    if (n > 1) {
	with(gprim)
	{
	    setStroke(c, theme['stroke']);
	    linesV(c, x, y);
	}
    }
}


function segments(x1, y1, x2, y2)
{
    var i, n = x.length, c = stages[theme['target']];
    with(gprim)
    {
	setStroke(c, theme['stroke']);
	segmentsV(c, x1, y1, x2, y2)
    }
}

// FIXME: filled rectangles will usually have different colors. Should
// make that easy in API. But even with this, R can make multiple
// calls to rect().
function rect(x0, y0, x1, y1)
{
    var i, n = x0.length, c = stages[theme['target']];
    var rectFun;
    with(gprim)
    {
	setStroke(c, theme['stroke']);
	setFill(c, theme['fill']);
	if (theme['fill'] !== false) rectFun = rectangleEmpty;
	else rectFun = rectangleFilled;
	for (i = 0; i < n; i++)
	    rectFun(c, x0[i], y0[i], x1[i], y1[i]);
    }
}

function polygon(x, y)
{
    var i, n = x.length, c = stages[theme['target']];
    var polyFun;;
    if (n > 1) {
	with(gprim)
	{
	    setStroke(c, theme['stroke']);
	    setFill(c, theme['fill']);
	    if (theme['fill'] !== false) polyFun = polygonEmpty;
	    else polyFun = polygonFilled;
	    polyFun(c, x, y);
	}
    }
}

function text(x, y, str, rot)
{
    // only scalar for now
    var i, n = x.length, c = stages[theme['target']];
    if (!rot) rot = 0;
    with(gprim)
    {
	setFont(c, theme['family'], theme['fill'], theme['ps'], theme['cex']);
	text1(c, x, y, str, -rot);
    }
}

function textdims(str)
{
    var h, w, c = stages[theme['target']];
    with(gprim)
    {
	setFont(c, theme['family'], theme['fill'], theme['ps'], theme['cex']);
	w = strwidth(c, str);
	h = theme['ps'] * theme['cex'];
    }
    ws.send("c(" + w + "," + h + ")"); // sent to R
}

function clip : function(xleft, ybottom, xright, ytop) 
{
    var c = stages[theme['target']]
    c.save();
    with(gprim)
    {
	clipRect(c, xleft, ybottom, xright, ytop);
    }
}

function unclip : function() 
{
    var c = stages[theme['target']]
    c.restore();
    // clipping to larger area does not work
}


function newpage()
{
    with(gprim)
    {
	clear(stages[theme['target']]);
    }
}

// function update()
// {
//     renderer.render(stages[theme['target']]);
// }

