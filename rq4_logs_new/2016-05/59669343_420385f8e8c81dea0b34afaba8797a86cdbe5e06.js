angular.module("services-quizz", [])
	.service('quizzDBUtils', function() {
		return {

			storeFieldName: '__quizz_answers',
			answers: [],

			writeToEngine: function() {
				localStorage.setItem(this.storeFieldName, JSON.stringify(this.answers));
			},

			readFromEngine: function() {
				var answers = localStorage.getItem(this.storeFieldName);
				return answers? JSON.parse( answers ) : [];
			},

			setAnswers: function(answers) {
				this.answers = answers;
				this.writeToEngine();
			},

			getAnswers: function() {
				this.answers = this.readFromEngine();
				return this.answers;
			},

			resetAnswers: function() {
				this.answers = [];
				this.writeToEngine();
			},

			isNullAnswers: function() {
				return this.getCurrentIndex() === 0;
			},

			getCurrentIndex: function() {
				return this.answers.reduce(function(previous, current) {
					if ( current.selectedAnswer === undefined ) {
						return previous.selectedAnswer? previous.selectedAnswer : 0;
					}
				});
			}
		}
});