class Greeter {
    element: HTMLElement;
    span: HTMLElement;
    timerToken: number;

    constructor(element: HTMLElement) {
	this.element = element;
	this.element.innerHTML += "The time is: ";
	this.span = document.createElement('span');
	this.element.appendChild(this.span);
	this.span.innerText = new Date().toUTCString();
    }

    start() {
	this.timerToken = setInterval(() => {
	    this.span.innerHTML = new Date().toUTCString();
	    this.span.style.backgroundColor = getRandomColor();
	}, 500);
    }

    stop() {
	clearTimeout(this.timerToken);
    }
}

window.onload = () => {
    var el = document.getElementById('content');
    var greeter = new Greeter(el);
    greeter.start();
};

function getRandomColor() {
    var letters = '0123456789ABCDEF'.split('');
    var color = '#';
    for (var i = 0; i < 6; i++)
	color += letters[Math.floor(Math.random() * 16)];
    return color;
}