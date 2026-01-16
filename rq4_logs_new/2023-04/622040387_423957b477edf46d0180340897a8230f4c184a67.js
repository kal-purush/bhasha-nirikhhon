function scriptTest() {
    alert("Hey my Javascript is working");
}

const months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
const Days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
function getTime() {
    let currDate = new Date();

    //Pull all the info from currDate
    let cDate = currDate.getDate();
    let cDay = Days[currDate.getDay()];
    let cMonth = months[currDate.getMonth()];
    let cYear = currDate.getFullYear();

    //Recieving all info from time 
    let currHour = currDate.getHours();
    let suffix = "am";
    //Checking for am or pm
    if(currHour > 12){
        suffix = "pm";
        currHour = currHour - 12;
    }

    let currMin = currDate.getMinutes();

    //Formatting minutes 
    if(currMin < 10){
        currMin = "0" +currMin;
    }

    //String to be returned 

    return "Today is "+cDay+ ", " + cMonth + " " + cDate + " " + cYear + ". It is currently " + currHour + ":" + currMin + suffix;
}
function displayTime(){
    //Getting the String
    let text = getTime();

    //Outputing it to HTML
    let area = document.getElementById("TimeArea");
    area.innerHTML = text;
    return;
}

function getMessageInfo(){
    //Getting the Message information
    let fname = document.getElementById("fname").value;
    let lname = document.getElementById("lname").value;
    let mood = document.getElementById("mood").value;

    if(!fname){
        fname= "wow";
    }
    if(!lname){
        lname = "this one too";
    }
    if(!mood){
        mood = "can't believe this";
    }

    return "Welcome to the Drowsy Panda " + fname + " " + lname + ". I see you've been feeling " + mood + " recently, huh?";
}

function displayMessage(){
    //Getting Message 
    let info = getMessageInfo();
    //Printing it out 
    let area = document.getElementById("messageArea");
    area.innerHTML = info;
    return;
}

function clearMessage(){
    let text = "Fill out the Form....... Please?"

    let area = document.getElementById("messageArea")
    area.innerHTML = text;
    return;
}

function pandaSays(){
    alert("Zzzzzzzz.........");
}

function sleepInterval(){
    //getting the value of selected box
    let range = document.getElementById("ageRange").value;
    let textbubble = "null";
    //Running it through possible cases 
    switch(range){
        case 1 : textbubble = "Don't select this one bro come on";
                break;
        case 2: textbubble = "14-17 hours";
                break;
        case 3: textbubble = "12-16 hours";
                break;
        case 4: textbubble = "11-14 hours";
                break;
        case 5: textbubble = "10-13 hours";
                break;
        case 6: textbubble = "9-12 hours";
                break;
        case 7: textbubble = "8-10 hours";
                break;
        case 8: textbubble = "7 or more hours";
                break;
        default: textbubble = "how?";
                break;
        }
        return "The Drowsy Panda says you need " + textbubble + " of sleep everynight";
}

function printSleepInterval(){
    //getting print statement 
    let text = sleepInterval();

    //Printing out to html
    let area = document.getElementById("sleepSchedule");
    area.innerHTML = text;
    return;
}