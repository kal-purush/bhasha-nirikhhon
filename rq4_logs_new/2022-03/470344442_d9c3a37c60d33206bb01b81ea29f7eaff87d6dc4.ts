import mongoose from 'mongoose';
import type { Admin } from '../../shared/models/admin.model.js';
const {Schema, model} = mongoose

const adminSchema = new Schema<Admin>({
    firstname: {type: String, required: true},
    lastname: {type: String, required: true},
    email: {type: String, required: true },
    encryptedPassword: {type: String, required: true },
    role: {type: String, enum: ['admin', 'restricted'], required: true}
})

export const AdminModel = model<Admin>('Admin',adminSchema)