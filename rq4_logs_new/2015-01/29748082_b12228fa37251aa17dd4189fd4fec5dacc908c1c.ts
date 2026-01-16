var chunks = [
    [
        [0, 0, 0, 0],
        [0, 0, 0, 0],
        [0, 0, 0, 0],
        [0, 0, 0, 0],
        [0, 0, 0, 0],
        [1, 1, 1, 1],
    ]
];

/*
0: empty
1: solid
 */

class Chunk {
    data: number[][];
    offset: number;
    constructor(offset: number) {
        this.data = chunks[Math.random() * chunks.length];
        this.offset = offset;
    }
    render() {
        for(var row in this.data) {
            for (var tile in row) {
                if(tile != 0) {
                    // draw sprite
                }
            }
        }
    }
}