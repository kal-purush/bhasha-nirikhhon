function findFirstInClassList(element, condition) {
    for (let i = 0; i < element.classList.length; i++) {
        let item = element.classList[i];
        if (condition(item)) return item;
    }
    return undefined;
}

function createPiece(type) {
    let piece = document.createElement("div");
    piece.classList.add("piece");
    piece.classList.add(type);
    return piece;
}

function setPiecePosition(piece, squareNumber) {
    let currentPosition = findFirstInClassList(piece, (item) => item.startsWith("square-"));
    piece.classList.remove(currentPosition);
    piece.classList.add(`square-${squareNumber}`);
}

function elementFromType(type) {
    if (type == "  ") {
        return undefined;
    }
    return createPiece(type);
}

const plan = [
    "br", "bn", "bb", "bk", "bq", "bb", "bn", "br",
    "bp", "bp", "bp", "bp", "bp", "bp", "bp", "bp",
    "  ", "  ", "  ", "  ", "  ", "  ", "  ", "  ",
    "  ", "  ", "  ", "  ", "  ", "  ", "  ", "  ",
    "  ", "  ", "  ", "  ", "  ", "  ", "  ", "  ",
    "  ", "  ", "  ", "  ", "  ", "  ", "  ", "  ",
    "wp", "wp", "wp", "wp", "wp", "wp", "wp", "wp",
    "wr", "wn", "wb", "wk", "wq", "wb", "wn", "wr"
];

const board = document.getElementById("board");
for (let i = 0; i < plan.length; i++) {
    let element = elementFromType(plan[i]);
    if (element) {
        setPiecePosition(element, i);
        board.appendChild(element);
    }
}