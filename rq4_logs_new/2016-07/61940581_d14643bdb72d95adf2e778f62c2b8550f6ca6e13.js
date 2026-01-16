var totalScore = 0;
var timeScoreCat1 = 80;  // below 6 seconds
var timeScoreCat2 = 50;  // from 6 to 10 seconds
var timeScoreCat3 = 35;  // from 10 to 15 secods
var timeScoreCat4 = 20;  // from 15 to 20 seconds
var timeScoreCat5 = 10;  // more than 20 seconds

var ScoringController = {
	handleTimeLapsedInSec : function(timeLapsedInSec){
		if (timeLapsedInSec <= 6) {
			totalScore += timeScoreCat1;
			return totalScore;
		}
		else if (timeLapsedInSec > 6 && timeLapsedInSec <= 10) {
			totalScore += timeScoreCat2;
			return totalScore;
		}
		else if (timeLapsedInSec > 10 && timeLapsedInSec <= 15) {
			totalScore += timeScoreCat3;
			return totalScore;
		}
		else if (timeLapsedInSec > 15 && timeLapsedInSec <= 20) {
			totalScore += timeScoreCat4;
			return totalScore;
		}
		else if (timeLapsedInSec > 20) {
			totalScore += timeScoreCat5;
			return totalScore;
		}
		return totalScore;
	}
}