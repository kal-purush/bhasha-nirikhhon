function calculateCurrentGrade(){
    //these variables go to the functions below to allow them to work for all sections.
    var hwString = document.getElementById("homework").value;
    var totalHW = convertArrayStringToNumber(hwString);
    var hwWeight = parseFloat(document.getElementById("homeworkWeight").value) / 100;
    var colorBlockHW = document.getElementsByClassName("hwColor");
    var averageHW = averageArray(hwString, totalHW, hwWeight, colorBlockHW);
    console.log(averageHW);

    var quizString = document.getElementById("quizzes").value;
    var totalQuiz = convertArrayStringToNumber(quizString);
    var quizWeight = parseFloat(document.getElementById("quizWeight").value) / 100;
    var colorBlockQuiz = document.getElementsByClassName("quizColor");
    var averageQuiz = averageArray(quizString, totalQuiz, quizWeight, colorBlockQuiz);
    console.log(averageQuiz);

    var testString = document.getElementById("tests").value;
    var totalTest = convertArrayStringToNumber(testString);
    var testWeight = parseFloat(document.getElementById("testWeight").value) / 100;
    var colorBlockTest = document.getElementsByClassName("testColor");
    var averageTest = averageArray(testString, totalTest, testWeight, colorBlockTest);
    console.log(averageTest);

    var midString = document.getElementById("midterms").value;
    var totalMid = convertArrayStringToNumber(midString);
    var midtWeight = parseFloat(document.getElementById("midtWeight").value) / 100;
    var colorBlockMid = document.getElementsByClassName("midtColor");
    var averageMid = averageArray(midString, totalMid, midtWeight, colorBlockMid);
    console.log(averageMid);

    /*
    All the weighted values of the sections are added together and divided by
    80, as the current grade is only 80% of the total grade including the final.
     */
    var finalWeight = parseInt(document.getElementById("finalWeight").value) / 100;

    if((hwWeight + quizWeight + testWeight + midtWeight + finalWeight) * 100  != 100) {
        document.getElementById("currentGradeIs").innerHTML = "ERROR: WEIGHTS OVER 100%";
    } else {
        var currentGrade = (averageHW + averageQuiz + averageTest + averageMid) / (hwWeight + quizWeight + testWeight + midtWeight);
        console.log(currentGrade);
        if(isNaN(currentGrade)){
            document.getElementById("currentGradeIs").innerHTML = "ERROR: INPUT NaN";
        } else {
            if(currentGrade >= 0 && currentGrade < 300) {
                document.getElementById("currentGradeIs").innerHTML = "You have a " +currentGrade.toPrecision(4) + " in this class!";
            } else {
                document.getElementById("currentGradeIs").innerHTML = "ERROR: INPUT UNREASONABLE";

            }

        }
    }
    return currentGrade;
}

function convertArrayStringToNumber(str){
    var splitStr = str.split(",");
    var total = 0;
    //Adds all values in the original input to find the total of all the grades in that section.
    for(var i = 0; i < splitStr.length; i++){
        total += parseFloat(splitStr[i]);
    }
    console.log(total);
    return total;
}


function averageArray(str, total, weight, color){
    /*This function finds the average of all of the grades
    in a certain subject, then multiplies it by the given weight,
    which gives the amount out of 80 instead of 100.
     */
    var splitStr = str.split(",");
    var amount = 0;
    for(var i = 0; i < splitStr.length; i++){
        amount += 1;
    }
    console.log(color);
    var average = total / amount;

    if(average >= 89.5) {
        color[0].style.background = "#008900";
        color[1].style.background = "#008900";
    }
    if(average >=79.5 && average < 89.5) {
        color[0].style.background = "#0be215";
        color[1].style.background = "#0be215";
    }
    if(average >= 69.5 && average < 79.5) {
        color[0].style.background = "#eeeb01";
        color[1].style.background = "#eeeb01";
    }
    if(average >= 59.5 && average < 69.5) {
        color[0].style.background = "#ffa207";
        color[1].style.background = "#ffa207";
    }
    if(average >= 0 && average < 59.5) {
        color[0].style.background = "#ff1b00";
        color[1].style.background = "#ff1b00";
    }

    var weighted = average * weight;
    console.log(weighted);
    return weighted;

}

function calculateGradeNeeded(nowGrade){
    /*
    This function finds the grade you need on the final exam to get the
    grade you want in the class.
     */

    var goalGrade = parseFloat(document.getElementById("goalGrade").value) / 100;
    console.log(goalGrade);
    var finalWeight = document.getElementById("finalWeight").value / 100;
    console.log(finalWeight);

    var hwWeight = parseFloat(document.getElementById("homeworkWeight").value) / 100;
    var quizWeight = parseFloat(document.getElementById("quizWeight").value) / 100;
    var testWeight = parseFloat(document.getElementById("testWeight").value) / 100;
    var midtWeight = parseFloat(document.getElementById("midtWeight").value) / 100;
    var allWeights = hwWeight + quizWeight + testWeight + midtWeight;
    console.log(allWeights);


    var currentGrade = nowGrade / 100;
    console.log(currentGrade);
    //calculates the grade needed based on the current grade and the desired grade.

    var gradeNeeded = ((goalGrade - (currentGrade * allWeights)) / finalWeight) * 100;

    console.log(gradeNeeded);
    document.getElementById("finalGradeNeeded").innerHTML = "You need a " +gradeNeeded.toPrecision(4) + " on the final to get a " + goalGrade.toPrecision(4) * 100 + " overall. Good luck!";
    return gradeNeeded;
}