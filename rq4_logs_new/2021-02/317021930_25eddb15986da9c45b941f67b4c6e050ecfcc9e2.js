import mongoose from 'mongoose'
import User from './userModels.js'

const adminLogSchema = mongoose.Schema({
    operator: {
        type: mongoose.Schema.Types.ObjectId,
        required: true,
        ref: User
    },
    activity: {
        type: String,
        required: true
    },
    customer: {
        type: String
    }
}, {
    timestamps: true
})

const AdminLog = mongoose.model("AdminLog", adminLogSchema)
export default AdminLog