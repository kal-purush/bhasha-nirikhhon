class ElementaryCellularAutomaton {
    constructor(rule, width) {
        this.rule = rule;
        this.cells = new Array(width).fill(false);
        this.cells[Math.floor(width / 2)] = true;
    }

    update() {
        const newCells = new Array(this.cells.length);
        for (let i = 0; i < this.cells.length; i++) {
            const left = this.getCell(i - 1);
            const center = this.getCell(i);
            const right = this.getCell(i + 1);

            newCells[i] = this.applyRule(left, center, right);
        }
        this.cells = newCells;
    }

    getCells() {
        return [...this.cells];
    }

    getCell(index) {
        if (index < 0 || index >= this.cells.length) {
            return false;
        }
        return this.cells[index];
    }

    applyRule(left, center, right) {
        const index = (left ? 4 : 0) + (center ? 2 : 0) + (right ? 1 : 0);
        const mask = 1 << index;
        // console.log("🚀 ~ file: cellular-autometa.js:34 ~ ElementaryCellularAutomaton ~ applyRule ~ this.rule :", this.rule)
        return (this.rule & mask) !== 0;
    }
}

const width = 51;
const automaton = new ElementaryCellularAutomaton(110, width);

for (let i = 0; i < 25; i++) {
    console.log(automaton.getCells().map(c => c ? "X" : " ").join(""));
    automaton.update();
}