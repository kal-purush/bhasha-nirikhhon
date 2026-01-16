var johnTeamGame1 = 140;
var johnTeamGame2 = 120;
var johnTeamGame3 = 103;

var markTeamGame1 = 116;
var markTeamGame2 = 94;
var markTeamGame3 = 123;

// 1. Calculate average score of each team

var johnAveScore = (johnTeamGame1 + johnTeamGame2 + johnTeamGame3)/3;
console.log(johnAveScore);  //Average = 104

var markAveScore = (markTeamGame1 + markTeamGame2 + markTeamGame3)/3;
console.log(markAveScore); //Average = 111

// 2. Check the winner through the highest average score and console log it 
if (johnAveScore > markAveScore){
    console.log('John\'s team is the winner with the highest average point of ' + johnAveScore);
} else if (johnAveScore === markAveScore) {
    console.log('Both teams have a draw with '+ johnAveScore + ', ' + markAveScore + ' points each!!');
} else {
    console.log('Mark\'s team is the winner with the highest average point of ' + markAveScore);
};