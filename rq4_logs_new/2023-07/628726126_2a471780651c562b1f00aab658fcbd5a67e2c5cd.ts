import {
    Document,
    HydratedDocument,
    Schema as MongooseSchema,
    Types,
} from 'mongoose';
import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

export type MessageDocument = HydratedDocument<Message>;

@Schema({ versionKey: false, timestamps: true })
export class Message extends Document {
    @Prop({ required: true, type: MongooseSchema.Types.String })
    text: string;

    @Prop({
        required: true,
        type: [{ type: MongooseSchema.Types.ObjectId, ref: 'User' }],
    })
    from: Types.ObjectId[];
}

export const MessageSchema = SchemaFactory.createForClass(Message);