class Aim {
    constructor({
        screenSelector,
        startSelector,
        changeScreenClass,
        btnList,
        timerSelector,
        boardSelector,
        itemClass,
    }) {
        this.screens = document.querySelectorAll(screenSelector);
        this.startBtn = document.querySelector(startSelector);
        this.changeScreenClass = changeScreenClass;
        this.btnList = document.querySelector(btnList);
        this.timer = document.querySelector(timerSelector);
        this.board = document.querySelector(boardSelector);
        this.itemClass = itemClass;

        this.boardData = this.board.getBoundingClientRect();
        this.count = 0;

        this.time = 0;

        this.init();
    }

    getRandomNumber(min, max) {
        return Math.floor(Math.random() * max + min);
    }

    getRandomColor() {
        return "#" + Math.floor(Math.random() * 16777215).toString(16);
    }

    generateItem() {
        const item = document.createElement("div");

        const { width, height } = this.boardData;
        const size = this.getRandomNumber(10, 80);
        const x = this.getRandomNumber(0, width - size);
        const y = this.getRandomNumber(0, height - size);
        item.classList.add(this.itemClass);
        item.style.width = size + "px";
        item.style.height = size + "px";
        item.style.background = this.getRandomColor();
        item.style.top = y + "px";
        item.style.left = x + "px";
        this.board.append(item);
    }

    endGame() {
        this.board.innerHTML = `<h2>Score: ${this.count}</h2>`;
        this.timer.parentNode.classList.add("hide");
    }

    startGame(time) {
        this.time = time;
        this.timer.textContent = `00:${
            this.time < 10 ? "0" + this.time : this.time
        }`;
        this.generateItem();

        const interval = setInterval(() => {
            this.time--;
            this.timer.textContent = `00:${
                this.time < 10 ? "0" + this.time : this.time
            }`;
            if (this.time <= 0) {
                this.endGame();
                clearInterval(interval);
            }
        }, 1000);
    }

    addListeners() {
        this.startBtn.addEventListener("click", (evt) => {
            evt.preventDefault();
            this.screens[0].classList.add(this.changeScreenClass);
        });

        this.btnList.addEventListener("click", (evt) => {
            const target = evt.target;

            if (target.tagName === "BUTTON") {
                this.screens[1].classList.add(this.changeScreenClass);
                this.startGame(target.dataset.time);
            }
        });
        this.board.addEventListener("click", (evt) => {
            const target = evt.target;
            if (target.classList.contains(this.itemClass)) {
                target.remove();
                this.count++;
                this.generateItem();
            }
        });
    }

    init() {
        this.addListeners();
    }
}

new Aim({
    screenSelector: ".screen",
    startSelector: ".start",
    changeScreenClass: "up",
    btnList: ".time-list",
    timerSelector: "#time",
    boardSelector: "#board",
    itemClass: "circle",
});