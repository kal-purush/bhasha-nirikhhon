module.exports = function(mongoose) {

  // setup mongo connection
  var mongoUri = process.env.MONGOLAB_URI;
  mongoose.connect(mongoUri, function (err, res) {
    if (err) { 
      console.log ('ERROR connecting to: ' + mongoUri + '. ' + err);
    } else {
      console.log ('Succeeded connected to: ' + mongoUri);
    }
  });

  // setup models
  var questionSchema= new mongoose.Schema({
      num: {type: Number, unique: true},
      quiz: [String],
      question: { type: String, trim: true },
      correct_choice: Number, // TODO: this should probably be a String
      choices: [ String ],
      num_choices: Number, // TODO: Yes, this is dumb. Just it makes jade easier.
      footer: String
  });
  var Question = mongoose.model('Question', questionSchema);
  var choiceSchema= new mongoose.Schema({
      uid: Number,
      q_num: Number,
      q_choice: Number,
  });
  var Choice = mongoose.model('Choice', choiceSchema);
  var resultSchema= new mongoose.Schema({
      q_num: Number,
      question: String, // TODO: Redundant...but okay for now.
      choices: [{ choice: Number, votes: Number }] 
  });
  var Result = mongoose.model('Result', resultSchema);

  return {
    Question: Question,
    Choice: Choice,
    Result: Result,
  };
}