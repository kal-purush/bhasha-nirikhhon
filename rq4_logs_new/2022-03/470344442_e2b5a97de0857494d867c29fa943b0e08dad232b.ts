import mongoose from 'mongoose';
import { User } from '../../shared/models/user.model.js';
const {Schema, model} = mongoose

const UserSchema = new Schema<User>({
    firstname: {type: String, required: true},
    lastname: {type: String, required: true},
    email: {type: String, required: true },
    password: {type: String, required: true},
    points: {type: Number, required: true, default: 0}
})

export const UserModel = model<User>('User',UserSchema)