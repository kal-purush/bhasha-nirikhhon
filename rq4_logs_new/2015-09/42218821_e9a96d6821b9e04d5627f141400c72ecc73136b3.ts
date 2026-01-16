var ctx = (<HTMLCanvasElement>document.getElementById("animation")).getContext("2d");
var width = height = ctx.canvas.width = ctx.canvas.height = 500;
var max = 100;
var num = 20;
var alg = bubbleSort;
var indWidth = width / num;
//random array of 0-100, n long

var timers = []
var arr;
reset(); //reset array;
draw(); //draw first time.

makeSlideInput("num", v => {
    clearTimers()
    num = v;
    reset();
    indWidth = width / num;
    draw();
}, 1, 100, 1);

makeComboInput("type", v => {
    clearTimers()
    alg = getAlg(v)
    draw()
})


function clearTimers(){
    for(var i in timers) clearTimeout(timers[i])
}

function reset() {
    arr = [];
    for (var i = 0; i < num; i++)
        arr.push(Math.floor(Math.random() * max))

}
function getRandomColor() {
    var letters = '0123456789ABCDEF'.split('');
    var color = '#';
    for (var i = 0; i < 6; i++)
        color += letters[Math.floor(Math.random() * 16)];
    return color;
}
function drawAll(timeout?, msg?) {
    var arrClone = arr.slice();
    if (timeout != null)
        timers.push(setTimeout(() => {
            ctx.fillText(msg, 0, height * .75)
            ctx.fillStyle = "white";
            ctx.fillRect(0, 0, width, width)

            arrClone.forEach((v, i) => {
                ctx.fillStyle = scaledColor(v, max);
                var _v = (v / max)
                ctx.beginPath();
                ctx.moveTo(i * indWidth, height);
                ctx.lineTo((i * indWidth) + indWidth, height);
                ctx.lineTo((i * indWidth) + indWidth, height * _v);
                ctx.lineTo(i * indWidth, height * _v);
                ctx.lineTo(i * indWidth, height);
                ctx.fill();
                ctx.stroke();
            })
        }, timeout))
}
function scaledColor(a, b) {
    var c = Math.round(100 * (a / b));
    return `hsl(${c},60%,60%)`;
}
function draw(delay, msg?) {
    //draw the current state of the array.
    drawAll((delay * 100) || 0, msg);

    if (delay == null)
        alg(arr);
}

function compareFn(n1, n2) {
    return n1 - n2;
}

function bubbleSort(arr, cmp?) {
    var iter = 0;
    for (var i = 0; i < arr.length; i++ , iter++)
        for (var j = i; j > 0; j-- , iter++)
            if (((cmp || compareFn)(arr[j], arr[j - 1])) < 0) {
                var tmp = arr[j - 1];
                arr[j - 1] = arr[j];
                arr[j] = tmp;
                draw(iter)
            }
    return arr;
};

function makeSlideInput(id, callbackOnChange, min, max, step?) {
    var el: any = document.getElementById(id)
    if (!el) {
        console.error(id + " doesn't exist")
        return;
    }
    el.addEventListener("change", function(evt) {
        callbackOnChange((<HTMLTextAreaElement>evt.srcElement).value);
        draw()
    })
    if (step) el.step = step;
    el.min = min || "";
    el.max = max || "";
    el.style.width = "500px";
}

function makeComboInput(id, callbackOnChange) {
    var el: any = document.getElementById(id)
    if (!el)
        console.error(id + " doesn't exist")
    else el.addEventListener("change", function(evt) {
        callbackOnChange((<HTMLTextAreaElement>evt.srcElement).value);
        draw();
    })
}

function getAlg(v: string) {
    if (v === "bubble") return bubbleSort
    //if (v === "selection") return selectionSort
    //if (v === "insert") return insertionSort
    if (v === "quick") return quicksort
    //if (v === "merge") return mergeSort
}


function partition(items, left, right) {
    var pivot   = items[Math.floor((right + left) / 2)],
        i       = left,
        j       = right;
    while (i <= j) {
        while (items[i] < pivot)
            i++;
        while (items[j] > pivot)
            j--;
        if (i <= j) {
            var tmp = items[i]
            items[i++] = items[j];
            items[j--] = tmp;
        }
    }
    return i;
}

function quicksort(items, left?:number, right?:number, i?,j?) {
    var index;
    i = i || 0;
    i = j || 0;
    draw(i*j + i, "Quicksort")
    if (items.length > 1) {
        index = partition(items, left, right);
        if (left < index - 1)
            quicksort(items, left, index - 1, i+1, j);
        if (index < right)
            quicksort(items, index, right, i+1, j+1);
    }
    return items;
}


function mergeSort() {
    var length = this.length,
        middle = Math.floor(length / 2);

    if (length < 2)
        return this;

    return merge(
        this.slice(0, middle).mergeSort(compareFn),
        this.slice(middle, length).mergeSort(compareFn)
    );
}

function merge(left, right) {
    var result = [];

    while (left.length > 0 || right.length > 0) {
        if (left.length > 0 && right.length > 0) {
            if (compareFn(left[0], right[0]) <= 0) {
                result.push(left[0]);
                left = left.slice(1);
            }
            else {
                result.push(right[0]);
                right = right.slice(1);
            }
        }
        else if (left.length > 0) {
            result.push(left[0]);
            left = left.slice(1);
        }
        else if (right.length > 0) {
            result.push(right[0]);
            right = right.slice(1);
        }
    }
    return result;
}