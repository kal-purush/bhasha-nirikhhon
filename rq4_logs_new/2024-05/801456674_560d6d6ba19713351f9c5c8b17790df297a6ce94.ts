import { Schema, model } from "mongoose";
import { UserInterface } from "./user.interface";


const UserSchema = new Schema<UserInterface>({

    id: {
        type: String, required: true
    }
    ,
    password: { type: String, required: true },
    needsPasswordChange: { type: Boolean },
    role: {
        type: String,
        enum: ['student', 'faculty', 'admin']
    },
    status: {
        type: String,
        enum: {
            values: ['in-progress', 'blocked']
        }
    },
    isDeleted: {
        type: Boolean,
        default: false,
    },




},
    {
        timestamps: true
    })



const UserModel = model<UserInterface>("User", UserSchema)

export default UserModel