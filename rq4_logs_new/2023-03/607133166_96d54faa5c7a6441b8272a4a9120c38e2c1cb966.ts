import mongoose from "mongoose";

let todoSchema = new mongoose.Schema({
    task: {
        type: String,
        required: true,
    },
    priority: {
        type: String,
        enum : ['High','Medium',"Low"],
        required: true,
    },
    dueDate: {
        type: Date,
        required: true, 
    },
    done: {
        type: Boolean,
        required:true
    },
},
{
    timestamps:true
});

let todoCol = mongoose.models.todoCollection || mongoose.model("todoCollection", todoSchema);

export default todoCol;