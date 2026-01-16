var mongoose = require('mongoose');

var quizzesSchema = new mongoose.Schema({
    //properties of quiz go here
    quizzes: {
        type: [
            'Mixed'
        ]
    }
})

const Quizzes = module.exports = mongoose.model('Quizzes', quizzesSchema);

module.exports.getById = (id, callback) => {
    var query = {quizId: id};
    console.log(id + '= id from the quiz model')
    Quizzes.findbyId(query, callback);
}