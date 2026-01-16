import { GridPointClass } from "./a-star";

type TileSymbolType= "-" | "_"

export function swapSymbolForGridNode({ map }: { map: string[][] }) {
  let newGridMap: GridPointClass[][] = [];

  map.forEach((row, rowIndex) => {
    row.forEach((symbol, columnIndex) => {
      switch (symbol) {
        // pipe horizontal
        case "-":
          newGridMap[rowIndex][columnIndex] = new GridPointClass(
            rowIndex,
            columnIndex
          );
          break;

        // pipe vertical
        case "|":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: Boundary.cellWidth * columnIndex,
                y: Boundary.cellHeight * rowIndex,
              },
              spriteIndex: [7, 5],
            })
          );
          break;

        // pipe corner 1 - upper left
        case "1":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: Boundary.cellWidth * columnIndex,
                y: Boundary.cellHeight * rowIndex,
              },
              // image: createImage("./src/image/pipeCorner1.png"),
              spriteIndex: [9, 3],
            })
          );
          break;

        // Pipe corner 2 - upper right
        case "2":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: Boundary.cellWidth * columnIndex,
                y: Boundary.cellHeight * rowIndex,
              },
              // image: createImage("./src/image/pipeCorner2.png"),
              spriteIndex: [9, 4],
            })
          );
          break;

        // Pipe corner 3 - bottom right
        case "3":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: Boundary.cellWidth * columnIndex,
                y: Boundary.cellHeight * rowIndex,
              },
              // image: createImage("./src/image/pipeCorner3.png"),
              spriteIndex: [7, 4],
            })
          );
          break;

        // Pipe corner 4 - bottom left
        case "4":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: Boundary.cellWidth * columnIndex,
                y: Boundary.cellHeight * rowIndex,
              },
              // image: createImage("./src/image/pipeCorner4.png"),
              spriteIndex: [8, 4],
            })
          );
          break;

        // Block - a one tile sized block
        case "b":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: Boundary.cellWidth * columnIndex,
                y: Boundary.cellHeight * rowIndex,
              },
              // image: createImage("./src/image/block.png"),
              spriteIndex: [9, 5],
            })
          );
          break;

        // Cap left - a cap for one tile sized open entities
        case "[":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              // image: createImage("./src/image/capLeft.png"),
              spriteIndex: [4, 4],
            })
          );
          break;
        case "]":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              spriteIndex: spriteEntities.capRight,
            })
          );
          break;
        case "_":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              // image: createImage("./src/image/capBottom.png"),
              spriteIndex: spriteEntities.capBottom, // [3, 4],
            })
          );
          break;
        case "^":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              // image: createImage("./src/image/capTop.png"),
              spriteIndex: [8, 5],
            })
          );
          break;
        case "+":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              // image: createImage("./src/image/pipeCross.png"),
              spriteIndex: spriteEntities.pipeCross, // [6, 5],
            })
          );
          break;
        case "5":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              spriteIndex: [8, 3],
            })
          );
          break;
        case "6":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              // image: createImage("./src/image/pipeConnectorRight.png"),
              spriteIndex: [5, 5],
            })
          );
          break;
        case "7":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              // image: createImage("./src/image/pipeConnectorBottom.png"),
              spriteIndex: spriteEntities.pipeConnectorBottom,
            })
          );
          break;
        case "8":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              // image: createImage("./src/image/pipeConnectorLeft.png"),
              spriteIndex: spriteEntities.pipeConnectorLeft, //
            })
          );
          break;
        case ".":
          pellets.push(
            new Pellet({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth + Boundary.cellWidth / 2,
                y: rowIndex * Boundary.cellHeight + Boundary.cellHeight / 2,
              },
            })
          );
          break;

        case "p":
          powerUps.push(
            new PowerUp({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth + Boundary.cellWidth / 2,
                y: rowIndex * Boundary.cellHeight + Boundary.cellHeight / 2,
              },
            })
          );
          break;

        // Upper left corner boundary
        case "ul":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              spriteIndex: spriteEntities.upperLeftHalfBlock, // [9, 1],
            })
          );
          break;

        // Upper right corner boundary
        case "ur":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              spriteIndex: spriteEntities.upperRighttHalfBlock, // [9, 1],
            })
          );
          break;

        // Bottom left corner boundary
        case "bl":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              spriteIndex: spriteEntities.bottomLeftHalfBlock, // [9, 1],
            })
          );
          break;

        // Bottom right corner boundary
        case "br":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              spriteIndex: spriteEntities.bottomRightHalfBlock, // [9, 1],
            })
          );
          break;

        // Top of the tile only boundary (a line)
        case "to":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              spriteIndex: spriteEntities.topOnlyBar, // [9, 1],
            })
          );
          break;

        // Bottom of the tile oriented boundary (a line)
        case "bo":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              spriteIndex: spriteEntities.bottomOnlyBar, // [9, 1],
            })
          );
          break;

        // Left of the tile oriented boundary (a line)
        case "lo":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              spriteIndex: spriteEntities.leftOnlyBar, // [9, 1],
            })
          );
          break;

        // Right of the tile oriented boundary (a line)
        case "ro":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              spriteIndex: spriteEntities.rightOnlyBar,
            })
          );
          break;

        // Bottom of the tile oriented boundary (a line)
        case "gg":
          boundaries.push(
            new Boundary({
              context,
              position: {
                x: columnIndex * Boundary.cellWidth,
                y: rowIndex * Boundary.cellHeight,
              },
              spriteIndex: spriteEntities.ghostGate, // [9, 1],
            })
          );
          break;
      }
    });
  });
}