//
// ctx
//

let ctx = null;


//
// ttt
//

let ttt = {
    a: 60,
    marks: [], // 3x3 array of null/true/false representing blank/knot/cross
};

for (let i = 0; i < 3; ++i) {
    let row = [];
    for (let j = 0; j < 3; ++j) {
        row.push(true);
    }
    ttt.marks.push(row);
}


//
// redraw
// draw_grid
// draw_mark
//

function redraw() {
    let w = ctx.canvas.width;
    let h = ctx.canvas.height;
    ctx.translate(w/2, h/2);
    ctx.scale(1, -1);
    draw_grid();
    for (let i = 0; i < 3; ++i) {
        let row = ttt.marks[i];
        for (let j = 0; j < 3; ++j) {
            draw_mark(i, j, row[j]);
        }
    }
}

function draw_grid() {
    ctx.fillStyle = "black";
    ctx.fillRect(-ttt.a/2 - 1, -3*ttt.a/2, 2, 3*ttt.a);
    ctx.fillRect(ttt.a/2 - 1, -3*ttt.a/2, 2, 3*ttt.a);
    ctx.fillRect(-3*ttt.a/2, -ttt.a/2 - 1, 3*ttt.a, 2);
    ctx.fillRect(-3*ttt.a/2, ttt.a/2 - 1, 3*ttt.a, 2);
}

function draw_mark(i, j, mark) {
    ctx.fillStyle = "blue";
    switch (mark) {
    case null: {
        break;
    }
    case true: {
        // TODO: Draw O at i, j.
    }
    case false: {
        // TODO: Draw X at i, j.
    }
    }
}


//
// DOMContentLoaded
// _on_resize
//

function _on_resize() {
    let root = ctx.canvas.parentElement;
    ctx.canvas.style.width = root.clientWidth + "px";
    ctx.canvas.style.height = root.clientHeight + "px";
    ctx.canvas.width = ctx.canvas.clientWidth;
    ctx.canvas.height = ctx.canvas.clientHeight;
}

window.addEventListener("DOMContentLoaded", () => {
    ctx = document
        .getElementById("root")
        .appendChild(document.createElement("canvas"))
        .getContext("2d");

    window.addEventListener("resize", _on_resize);
    _on_resize();

    redraw();

    ctx.canvas.addEventListener("mousemove", (event) => {
    });
    ctx.canvas.addEventListener("click", (event) => {
    });
});