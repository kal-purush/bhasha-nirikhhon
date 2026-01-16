import { radians } from '@apestaartje/geometry/dist/angle';
import { Transform } from '@apestaartje/geometry/dist/transform';
import { Vector } from '@apestaartje/geometry/dist/vector';

import { getCenter } from '@tetris/tetromino/getCenter';
import { Type } from '@tetris/tetromino/Type';

const CENTER_OFFSET: number = 0.5;

export class Tetromino {
    private readonly _type: Type;
    private readonly _blocks: Vector[];
    private readonly _center: Vector;

    public get blocks(): Vector[] {
        return this._blocks;
    }

    public get type(): Type {
        return this._type;
    }

    public constructor(type: Type, blocks: Vector[], center?: Vector) {
        this._type = type;
        this._blocks = blocks;

        if (center === undefined) {
            this._center = getCenter(blocks);
        }
    }

    public rotate(degrees: number): Tetromino {
        const transform: Transform = new Transform();

        transform.translate(
            this._center.x,
            this._center.y,
        );
        transform.rotate(radians(degrees));
        transform.translate(
            -this._center.x,
            -this._center.y,
        );

        const blocks: Vector[] = this._blocks.map((block: Vector): Vector => {
            block.x += CENTER_OFFSET;
            block.y += CENTER_OFFSET;

            const transformed: Vector = transform.transformPoint(block);

            transformed.x -= CENTER_OFFSET;
            transformed.y -= CENTER_OFFSET;

            return transformed;
        });

        return new Tetromino(this._type, blocks, this._center);
    }
}