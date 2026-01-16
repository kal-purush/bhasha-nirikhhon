var getQuestion = "What's your favorite color?";
var qType;


var GetQuestion = function GetQuestion() {

    getQuestion = "What's your favorite color?";
    
    CreateQuestion(getQuestion);

}

function CreateQuestion(argument) {
    
    //var pickPuzzle = Math.floor(Math.random() * 3);
    var pickPuzzle = 0;
    var deconstructed;
    
    switch(pickPuzzle) {
        
        case 0:
            qType = "Fill in the Blank"
            
            deconstructed = getQuestion.split(" ")
            var toDelete = Math.round(deconstructed.length/2);
            var startPoint = Math.floor(Math.random() * (toDelete - 1));
            
            for (var i = 0; i < deconstructed.length; i++) {
                
               if (i == startPoint && i <= (startPoint + (toDelete - 1)))  {
                   
                   getQuestion.replace(i, "_____");
                   
               }
                
                
            }
            
            
            //getQuestion = getQuestion.splice(startPoint, toDelete, "_____");
        
            break;
        
        
    }
    
}