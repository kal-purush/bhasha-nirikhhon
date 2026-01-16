import {Schema, model} from 'mongoose'

let fileSchema = new Schema({
    _id: { type: Schema.Types.ObjectId, auto: true },
    key: { type: string, required: true },
    date: { type: Date, default: new Date() },
    meta: { type: {
        mime: { type: String },
        size: { type: String }
    }}
}, { collection: 'files', versionKey: false });

export default model('File', fileSchema);