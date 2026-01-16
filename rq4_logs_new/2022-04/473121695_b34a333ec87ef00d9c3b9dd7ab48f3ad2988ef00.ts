import mongoose, { model, Schema } from 'mongoose';
const AutoIncrement = require('mongoose-sequence')(mongoose);


export interface inquiry {
    no?: number //AutoIncrement로 추가됨
    qacategory: string,
    title: string,
    content: string,
    userno: number,
    createdAt?: Date,
    parent?: number
}

const inquirySchema = new Schema<inquiry>({
    no: { type: Number, unique: true },
    qacategory: { type: String, required: true, }, //"교환", 환불, 배송, 상품문의, 주문취소, 주문/결제, 이벤트
    title: { type: String, required: true },
    content: { type: String, required: true },
    userno: { type: Number, required: true },
    createdAt: { type: Date, default: new Date() },
    parent: { type: Number }
})
if (!mongoose.models['inquiry']) {
    inquirySchema.plugin(AutoIncrement, { id: "inquiryno", inc_field: "no" })
}
const Inquiry = mongoose.models['inquiry'] ? model<inquiry>('inquiry') : model<inquiry>('inquiry', inquirySchema, 'inquiry')
export default Inquiry