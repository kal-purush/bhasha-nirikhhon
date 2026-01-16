import {
    HydratedDocument,
    Document,
    Schema as MongooseSchema,
    Types,
} from 'mongoose';
import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { User } from './user.schema';
import { Game } from './game.schema';

export type ScoreDocument = HydratedDocument<Score>;

@Schema({ versionKey: false, timestamps: true })
export class Score extends Document {
    @Prop({
        required: true,
        type: MongooseSchema.Types.ObjectId,
        ref: 'Game',
    })
    game: Types.ObjectId;

    @Prop({
        required: true,
        default: 1,
    })
    score: number;
}

export const ScoreSchema = SchemaFactory.createForClass(Score);