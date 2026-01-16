const size = 5;
const updateInterval = 25;
let boardModel: number[][];
let drawPosition = 0;

export function initCanvasPaint(context: CanvasRenderingContext2D) {
    createModel();
    initModel(context);
    window.setInterval(() => mainLoop(context), updateInterval);
}

function createModel() {
    boardModel = new Array(100).fill(undefined).map(createColumn);
}

function createColumn() {
    return new Array(100).fill(undefined);
}

function mainLoop(context: CanvasRenderingContext2D) {
    updateModel(context);
    renderModel(context);
}

function updateDrawPosition() {
    const changeNumber = Math.random();
    if (changeNumber < 0.133) {
        if (drawPosition > 0) drawPosition--;
    } else if (changeNumber > 0.866) {
        if (drawPosition < 100) drawPosition++;
    }
}

function initModel(context: CanvasRenderingContext2D) {
    boardModel.forEach((column, columnIndex) => {
        column[drawPosition] = 1;
        updateDrawPosition();
    });
}

function updateModel(context: CanvasRenderingContext2D) {
    boardModel.shift();
    updateDrawPosition();
    const newColumn = createColumn();
    newColumn[drawPosition] = 1;
    boardModel.push(newColumn);
}

function renderModel(context: CanvasRenderingContext2D) {
    boardModel.forEach((column, columnIndex) => {
        column.forEach((line, lineIndex) => {
            if (!!line) {
                context.fillStyle = '#000000';
            } else {
                context.fillStyle = '#333333';
            }
            context.fillRect(
                columnIndex * size,
                lineIndex * size,
                columnIndex * size + size,
                lineIndex * size + size
            );
        });
    });
}