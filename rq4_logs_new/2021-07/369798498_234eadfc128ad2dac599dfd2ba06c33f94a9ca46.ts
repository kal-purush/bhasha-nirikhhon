import { Schema, Prop, SchemaFactory } from '@nestjs/mongoose';
import * as mongoose from 'mongoose';

export type ImageDocument = Image & Document;

@Schema()
export class Image {
    @Prop()
    name: string;

    @Prop()
    description: string;

    @Prop()
    createdAt: Date;

    @Prop()
    img: {
        data: Buffer,
        contentType: String
    }
}

export const ImageSchema = SchemaFactory.createForClass(Image);