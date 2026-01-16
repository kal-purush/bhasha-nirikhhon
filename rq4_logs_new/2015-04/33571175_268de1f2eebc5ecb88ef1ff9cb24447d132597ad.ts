
var VERTEX_SHADER = `
    precision mediump float;

    attribute vec2 vertexPosition;

    uniform mat3 modelMatrix;
    uniform mat3 projectionMatrix;
    uniform mat3 texMatrix;

    varying vec2 texCoord;

    void main() {
        vec3 position = projectionMatrix * modelMatrix * vec3(vertexPosition, 1.0);
        gl_Position = vec4(position.xy, 0.0, 1.0);
        texCoord = (texMatrix * vec3(vertexPosition.x, 1.0 - vertexPosition.y, 1.0)).xy;
    }
`;

var FRAGMENT_SHADER = `
    precision mediump float;

    varying vec2 texCoord;

    uniform bool loaded;
    uniform sampler2D texture;

    void main() {
        if (loaded) {
            vec4 color = texture2D(texture, texCoord);
            gl_FragColor = color;
        } else {
            gl_FragColor = vec4(1.0, 0.0, 1.0, 1.0);
        }
    }
`;

var UNIFORM_MODEL_MATRIX: WebGLUniformLocation;
var UNIFORM_PROJECTION_MATRIX: WebGLUniformLocation;
var UNIFORM_LOADED: WebGLUniformLocation;
var UNIFORM_TEXTURE: WebGLUniformLocation;
var UNIFORM_TEX_MATRIX: WebGLUniformLocation;

function initWebGl(canvas: HTMLCanvasElement): WebGLRenderingContext {
    try {
        return (canvas.getContext("webgl") || canvas.getContext("experimental-webgl"));
    } catch (e) {
        return null;
    }
}

function compileShader(gl: WebGLRenderingContext, shaderType: number, shaderSource: string): WebGLShader {
    var shader = gl.createShader(shaderType);
    gl.shaderSource(shader, shaderSource);
    gl.compileShader(shader);
    if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
        console.log("Error while compiling shader");
        console.log(gl.getShaderInfoLog(shader));
    }
    return shader;
}

function initPipeline(gl: WebGLRenderingContext): void {
    var vertexShader = compileShader(gl, gl.VERTEX_SHADER, VERTEX_SHADER);
    var fragmentShader = compileShader(gl, gl.FRAGMENT_SHADER, FRAGMENT_SHADER);
    var program = gl.createProgram();
    gl.attachShader(program, vertexShader);
    gl.attachShader(program, fragmentShader);
    gl.linkProgram(program);
    if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
        console.log("Error while linking program");
        console.log(gl.getProgramInfoLog(program));
    }
    gl.useProgram(program);

    UNIFORM_MODEL_MATRIX = gl.getUniformLocation(program, "modelMatrix");
    UNIFORM_PROJECTION_MATRIX = gl.getUniformLocation(program, "projectionMatrix");
    UNIFORM_LOADED = gl.getUniformLocation(program, "loaded");
    UNIFORM_TEXTURE = gl.getUniformLocation(program, "texture");
    UNIFORM_TEX_MATRIX = gl.getUniformLocation(program, "texMatrix");

    var projectionMatrix = new Float32Array([
        // column major
        2.0, 0.0, 0.0,
        0.0, 2.0, 0.0,
        -1.0, -1.0, 1.0,
    ]);
    gl.uniformMatrix3fv(UNIFORM_PROJECTION_MATRIX, false, projectionMatrix);
    gl.uniform1i(UNIFORM_TEXTURE, 0);

    gl.clearColor(0.5, 0.5, 0.5, 1);

    gl.enable(gl.BLEND);
    gl.blendEquation(gl.FUNC_ADD);
    gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA);
}

window.onload = () => {
    var canvas = <HTMLCanvasElement> document.getElementById("glcanvas");
    var gl = initWebGl(canvas);
    if (!gl) {
        alert("WebGL not supported!");
        return;
    }
    initPipeline(gl);
    var game = new Game(gl);
    window.onkeydown = game.handleKeypress.bind(game);
    setInterval(game.tick.bind(game, gl), 1.0 / 60.0);
};

class Vec2 {
    constructor(public x: number = 0, public y: number = 0) {
    }
}

class Vec3 {
    constructor(public x: number = 0, public y: number = 0, public z: number = 0) {
    }
}

class Array2D<T> {
    private data: T[];
    private width_: number;
    private height_: number;
    
    constructor(width: number, height: number, init: T = null) {
        this.data = new Array<T>(width * height);
        for (var i = 0; i < this.data.length; ++i) {
            this.data[i] = init;
        }
        this.width_ = width;
        this.height_ = height;
    }

    get size(): Vec2 {
        return new Vec2(this.width, this.height);
    }

    get width(): number {
        return this.width_;
    }

    get height(): number {
        return this.height_;
    }

    get(x: number, y: number): T {
        if (x < 0 || x >= this.width) throw "Index out of bounds (" + x + "/" + y + ")";
        if (y < 0 || y >= this.height) throw "Index out of bounds (" + x + "/" + y + ")";
        return this.data[y * this.width + x];
    }

    set(x: number, y: number, value: T) {
        if (x < 0 || x >= this.width) throw "Index out of bounds (" + x + "/" + y + ")";
        if (y < 0 || y >= this.height) throw "Index out of bounds (" + x + "/" + y + ")";
        this.data[y * this.width + x] = value;
    }
}

class SpriteImage {
    private texture: WebGLTexture;

    private loaded: boolean;
    private image: HTMLImageElement;

    constructor(path: string) {
        this.texture = null;
        this.loaded = false;

        this.image = new Image();
        this.image.onload = () => {
            this.loaded = true;
        }
        this.image.src = path + ".png";
    }

    private upload(gl: WebGLRenderingContext): void {
        this.texture = gl.createTexture();
        gl.bindTexture(gl.TEXTURE_2D, this.texture);
        gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, this.image);
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR_MIPMAP_LINEAR);
        gl.generateMipmap(gl.TEXTURE_2D);
        this.image = null;
    }

    bind(gl: WebGLRenderingContext): void {
        if (this.loaded) {
            if (!this.texture) {
                this.upload(gl);
            }
            gl.activeTexture(gl.TEXTURE0);
            gl.bindTexture(gl.TEXTURE_2D, this.texture);
            gl.uniform1i(UNIFORM_LOADED, 1);
        } else {
            gl.uniform1i(UNIFORM_LOADED, 0);
        }
    }
}

class Sprite {
    static DUMMY_IMAGE = new SpriteImage("dummy");

    position: Vec3;  // absolute position [0; 1]
    size: Vec2;  // absolute size [0; 1]
    visible: boolean;  // is visible?
    image: SpriteImage;  // image to display
    textureSize: Vec2;  // absolute size of one texture tile (null allowed)

    constructor() {
        this.position = new Vec3();
        this.size = new Vec2(1, 1);
        this.visible = true;
        this.image = Sprite.DUMMY_IMAGE;
        this.textureSize = null;
    }
}

class SpriteRenderer {
    private buffer: WebGLBuffer;
    private modelMatrixUniform: WebGLUniformLocation;

    constructor(gl: WebGLRenderingContext) {
        var data = new Float32Array([
          //  x,   y,
            0.0, 0.0,
            1.0, 0.0,
            0.0, 1.0,
            1.0, 1.0,
        ]);

        this.buffer = gl.createBuffer();
        gl.bindBuffer(gl.ARRAY_BUFFER, this.buffer);
        gl.bufferData(gl.ARRAY_BUFFER, data, gl.STATIC_DRAW);
    }

    render(gl: WebGLRenderingContext, sprite: Sprite): void {
        if (!sprite.visible)
            return;

        var modelMatrix = new Float32Array([
            // column major!
            sprite.size.x,     0.0,               0.0,
            0.0,               sprite.size.y,     0.0,
            sprite.position.x, sprite.position.y, 1.0,
        ]);
        gl.uniformMatrix3fv(UNIFORM_MODEL_MATRIX, false, modelMatrix);

        var texX: number, texY: number;
        if (sprite.textureSize) {
            texX = sprite.textureSize.x;
            texY = sprite.textureSize.y;
        } else {
            texX = sprite.size.x;
            texY = sprite.size.y;
        }
        var texMatrix = new Float32Array([
            // column major!
            sprite.size.x / texX, 0.0, 0.0,
            0.0, sprite.size.y / texY, 0.0,
            0.0, 0.0, 1.0,
        ]);
        gl.uniformMatrix3fv(UNIFORM_TEX_MATRIX, false, texMatrix);

        sprite.image.bind(gl);

        gl.bindBuffer(gl.ARRAY_BUFFER, this.buffer);
        gl.enableVertexAttribArray(0);
        gl.vertexAttribPointer(0, 2, gl.FLOAT, false, 0, 0);
        gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
        gl.disableVertexAttribArray(0);
    }
}

class Game {
    static UNIT = 1 / 22;
    static WIDTH = 12;
    static HEIGHT = 21;

    static CELLS = [
        new SpriteImage("block1"),
        new SpriteImage("block2"),
        new SpriteImage("block3"),
        new SpriteImage("block4"),
    ];
    static BG = new SpriteImage("gamebg");
    static GAME_OVER = new SpriteImage("gameover");

    static PIECE_SIZE = 4;
    static PIECES = [
        [
            [
                [0, 1, 1, 0],
                [0, 1, 1, 0],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
            ].reverse(),
        ], [
            [
                [0, 0, 1, 0],
                [0, 1, 1, 0],
                [0, 1, 0, 0],
                [0, 0, 0, 0],
            ].reverse(), [
                [0, 1, 1, 0],
                [0, 0, 1, 1],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
            ].reverse(),
        ], [
            [
                [0, 1, 0, 0],
                [0, 1, 1, 0],
                [0, 0, 1, 0],
                [0, 0, 0, 0],
            ].reverse(), [
                [0, 0, 1, 1],
                [0, 1, 1, 0],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
            ].reverse(),
        ], [
            [
                [0, 1, 0, 0],
                [0, 1, 1, 0],
                [0, 1, 0, 0],
                [0, 0, 0, 0],
            ].reverse(), [
                [0, 1, 1, 1],
                [0, 0, 1, 0],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
            ].reverse(), [
                [0, 0, 1, 0],
                [0, 1, 1, 0],
                [0, 0, 1, 0],
                [0, 0, 0, 0],
            ].reverse(), [
                [0, 0, 1, 0],
                [0, 1, 1, 1],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
            ].reverse(),
        ], [
            [
                [0, 0, 1, 0],
                [0, 0, 1, 0],
                [0, 0, 1, 0],
                [0, 0, 1, 0],
            ].reverse(), [
                [1, 1, 1, 1],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
            ].reverse(),
        ], [
            [
                [0, 1, 0, 0],
                [0, 1, 0, 0],
                [0, 1, 1, 0],
                [0, 0, 0, 0],
            ].reverse(), [
                [0, 1, 1, 1],
                [0, 1, 0, 0],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
            ].reverse(), [
                [0, 1, 1, 0],
                [0, 0, 1, 0],
                [0, 0, 1, 0],
                [0, 0, 0, 0],
            ].reverse(), [
                [0, 0, 0, 1],
                [0, 1, 1, 1],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
            ].reverse(),
        ], [
            [
                [0, 0, 1, 0],
                [0, 0, 1, 0],
                [0, 1, 1, 0],
                [0, 0, 0, 0],
            ].reverse(), [
                [0, 1, 0, 0],
                [0, 1, 1, 1],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
            ].reverse(), [
                [0, 1, 1, 0],
                [0, 1, 0, 0],
                [0, 1, 0, 0],
                [0, 0, 0, 0],
            ].reverse(), [
                [0, 1, 1, 1],
                [0, 0, 0, 1],
                [0, 0, 0, 0],
                [0, 0, 0, 0],
            ].reverse(),
        ],
    ];

    private spriteRenderer: SpriteRenderer;
    private field: Array2D<number>;

    private spriteField: Sprite;
    private spritesCell: Sprite[];
    private spriteGameOver: Sprite;

    private currentPieceIdx: number;
    private currentPos: Vec2;
    private currentColor: number;
    private currentRotIdx: number;

    private gameOver: boolean;
    private moveTimeout: number;

    private get currentPiece(): number[][]{
        if (this.currentPieceIdx == null) {
            return null;
        } else {
            return Game.PIECES[this.currentPieceIdx][this.currentRotIdx];
        }
    }

    constructor(gl: WebGLRenderingContext) {
        this.spriteRenderer = new SpriteRenderer(gl);

        this.spriteField = new Sprite();
        this.spriteField.position = new Vec3(Game.UNIT, Game.UNIT, -1);
        this.spriteField.size = new Vec2(12 * Game.UNIT, 1 - Game.UNIT);
        this.spriteField.image = Game.BG;
        this.spriteField.textureSize = new Vec2(Game.UNIT * 2, Game.UNIT * 2);

        this.spritesCell = [];
        Game.CELLS.forEach((c) => {
            var sprite = new Sprite();
            sprite.image = c;
            sprite.size = new Vec2(Game.UNIT, Game.UNIT);
            this.spritesCell.push(sprite);
        });

        this.spriteGameOver = new Sprite();
        this.spriteGameOver.position = new Vec3(Game.UNIT * 2, Game.UNIT * 9, 1);
        this.spriteGameOver.size = new Vec2(Game.UNIT * 18, Game.UNIT * 4);
        this.spriteGameOver.image = Game.GAME_OVER;
        this.spriteGameOver.textureSize = new Vec2(Game.UNIT * 18 * (1024 / 576), Game.UNIT * 4 * (1024 / 128));


        this.field = new Array2D(Game.WIDTH, Game.HEIGHT, 0);
        this.currentPieceIdx = null;
        this.currentPos = new Vec2();
        this.gameOver = false;

        this.move();
    }

    private collisionTest(piece: number[][], pos: Vec2): boolean {
        for (var x = 0; x < Game.PIECE_SIZE; ++x) {
            for (var y = 0; y < Game.PIECE_SIZE; ++y) {
                if (piece[y][x] == 1) {
                    var theX = pos.x + x;
                    var theY = pos.y + y;
                    if (theX < 0 || theX >= Game.WIDTH ||
                        theY < 0 || theY >= Game.HEIGHT ||
                        this.field.get(theX, theY) != 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private rotatePiece(): void {
        // Clockwise rotation.
        var nextRotIdx = (this.currentRotIdx + 1) % Game.PIECES[this.currentPieceIdx].length;
        if (!this.collisionTest(Game.PIECES[this.currentPieceIdx][nextRotIdx], this.currentPos)) {
            this.currentRotIdx = nextRotIdx;
        }
    }

    private move(dropping: boolean = false): void {
        if (this.moveTimeout) {
            clearTimeout(this.moveTimeout);
        }

        do {
            if (this.currentPieceIdx == null) {

                dropping = false;  // Stop dropping if we are dropping

                // Spawn a piece.
                var pieceIdx = Math.floor(Math.random() * Game.PIECES.length);
                var colorIdx = Math.floor(Math.random() * Game.CELLS.length) + 1;
                var rotIdx = Math.floor(Math.random() * Game.PIECES[pieceIdx].length);

                this.currentPieceIdx = pieceIdx;
                this.currentPos.x = Math.floor((Game.WIDTH - Game.PIECE_SIZE) / 2);
                this.currentPos.y = Game.HEIGHT - Game.PIECE_SIZE;
                this.currentColor = colorIdx;
                this.currentRotIdx = rotIdx;
                
                // Game Over?
                if (this.collisionTest(this.currentPiece, this.currentPos)) {
                    this.gameOver = true;
                }

            } else {

                var nextPos = new Vec2(this.currentPos.x, this.currentPos.y - 1);
                if (this.collisionTest(this.currentPiece, nextPos)) {
                    // In case of a collision stop and realize the piece.
                    for (var x = 0; x < Game.PIECE_SIZE; ++x) {
                        for (var y = 0; y < Game.PIECE_SIZE; ++y) {
                            if (this.currentPiece[y][x] == 1) {
                                this.field.set(this.currentPos.x + x, this.currentPos.y + y, this.currentColor);
                            }
                        }
                    }
                    this.currentPieceIdx = null;
                } else {
                    // Otherwise move it down.
                    this.currentPos = nextPos;
                }

            }
        } while (dropping);

        if (!this.gameOver) {
            this.moveTimeout = setTimeout(this.move.bind(this), 1000);
        }
    }

    tick(gl: WebGLRenderingContext): void {
        gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);

        this.spriteRenderer.render(gl, this.spriteField);

        for (var x = 0; x < this.field.width; ++x) {
            for (var y = 0; y < this.field.height; ++y) {
                var value = this.field.get(x, y);
                if (value != 0) {
                    var idx = value - 1;
                    this.spritesCell[idx].position.x = (1 + x) * Game.UNIT;
                    this.spritesCell[idx].position.y = (1 + y) * Game.UNIT;
                    this.spriteRenderer.render(gl, this.spritesCell[idx]);
                }
            }
        }

        if (this.currentPiece) {
            for (var x = 0; x < Game.PIECE_SIZE; ++x) {
                for (var y = 0; y < Game.PIECE_SIZE; ++y) {
                    if (this.currentPiece[y][x] == 1) {
                        var idx = this.currentColor - 1;
                        this.spritesCell[idx].position.x = (1 + x + this.currentPos.x) * Game.UNIT;
                        this.spritesCell[idx].position.y = (1 + y + this.currentPos.y) * Game.UNIT;
                        this.spriteRenderer.render(gl, this.spritesCell[idx]);
                    }
                }
            }
        }

        if (this.gameOver) {
            this.spriteRenderer.render(gl, this.spriteGameOver);
        }
    }

    handleKeypress(ev: KeyboardEvent): void {
        switch (ev.key) {
            case "ArrowDown":
                if (!this.gameOver && this.currentPiece) {
                    this.move(true);
                }
                break;
            case "ArrowUp":
                if (!this.gameOver && this.currentPiece) {
                    this.rotatePiece();
                }
                break;
            case "ArrowLeft":
                if (!this.gameOver && this.currentPiece) {
                    var nextPos = new Vec2(this.currentPos.x - 1, this.currentPos.y);
                    if (!this.collisionTest(this.currentPiece, nextPos)) {
                        this.currentPos = nextPos;
                    }
                }
                break;
            case "ArrowRight":
                if (!this.gameOver && this.currentPiece) {
                    var nextPos = new Vec2(this.currentPos.x + 1, this.currentPos.y);
                    if (!this.collisionTest(this.currentPiece, nextPos)) {
                        this.currentPos = nextPos;
                    }
                }
                break;
            default:
                // Do not prevent the default.
                return;
        }

        ev.preventDefault();
    }
}