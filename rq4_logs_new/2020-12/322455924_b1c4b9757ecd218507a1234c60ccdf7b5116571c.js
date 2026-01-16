const mongoose = require('mongoose');

const Schema = mongoose.Schema;

const WorkoutSchema = new Schema ({
    
})






const User = mongoose.model("Workout", WorkoutSchema);

module.exports = User;