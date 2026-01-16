$(document).ready(function(){
    console.log("Here I am in the User Login Logic");
    let surveyId = 1;
    // retrieve the user id of the session storage username and store in the variable
    function handleSurveyAnswerStoring(){
        // A function for getting the survey questions associated to a survey id
        $.get("/api/surveyquestions/" + surveyId)
        .then(function(result){
            console.log(result);
            // Iterate over the survey questions associated to the survey id
            for(var key in result){
                let answerVal = $("#"+result[key].id).val().trim();
                console.log("This is the id of the question in the DB: ");
                console.log(result[key].id);
                console.log("This is the answer value user inputs: ");
                console.log(answerVal);
                singleQuestionAnswer(result[key].id,answerVal);
            };

        })
        .done(function(){
            window.location.replace("/matches");
        });
    };
    // Store a record in the surveyanswers table
    function singleQuestionAnswer(question,answer){
        var answerObj = {
            userKey: sessionStorage.getItem("user_id"),
            questionKey: parseInt(question),
            answerKey: parseInt(answer)
        };

        $.post("/api/surveyanswers",answerObj)
        .then(function(result){
            console.log("This is the object that will be passed to /api/surveyanswer route");
            console.log(answerObj);
        });
    };
    // Associate the survey answer storage to the complete survey button
    $("#survey-input").on("click", function(){
        event.preventDefault();
        handleSurveyAnswerStoring();
    });
});