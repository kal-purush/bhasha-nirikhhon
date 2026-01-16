/**
 * Created by session1 on 9/30/15.
 */
confirm("Welcome.");
var age = prompt("What is your age, player?");

switch(age) {
    case '14':
        console.log("You are young, but old enough. You may continue.");
        break;
    case '<=15':
        console.log("Please, continue.");
        break;
    default:
        console.log("Please, continue.");
}

console.log("The Keeper of Shadows has infiltrated your reality.");
console.log("If she wins the war, your reality will be like hers: a barren wasteland.");
var defend = prompt ("Do you wish to defend your world and fight against her?").toUpperCase();
switch(defend){
    case 'YES':
        if (userAnswer == "YES") {
            console.log("Take this: the Sword of Light. It will guide you with skill in battle.");
            console.log("It will help you defeat your enemies.");
        }
        //break?
    case 'NO':
    {
    else
        console.log("I am sorry but, you really haven't much of a choice at this point.");
        console.log("I understand you are afraid, but your world will fall without you.");
        console.log("Be brave, player.");
        console.log("Take this: The Sword of Light. It will guide you with its skill in battle.");
    }
        break;
    default:
        console.log("Take this: The Sword of Light. It will guide you with its skill in battle.");
        console.log("You have arrived at the Forest of Anniese, the location of the Keeper of Shadows.");
}

    var attack = prompt("Do you wish to strike now or strategize?").toUpperCase();
    switch(attack) {
        case 'STRIKE':
            if (userAnswer == "STRIKE") {
                console.log("You surprised her! You may have a chance to defeat her!");
                console.log("If you strike once more, that could be her downfall!");
                console.log("The Sword of Light, however, is weakening!");
                console.log("You haven't much time!");
            }

        case 'STRATEGIZE':
            else
        {

            console.log("Stealth may be your best bet! If you sneak up on her, you may get a good hit!");
            console.log("You got her!");
            console.log("You've surprised her!");
            console.log("You may have a chance to defeat her!");
            console.log("If you strike once more, that could be her downfall!");
            console.log("The Sword of Light, however, is weakening!");
            console.log("You haven't much time!");
        }
             break;
        default:
        console.log("You haven't much time!");
}
        var counterAttack = prompt("Strike her again and kill her or imprison her?");
        switch (counterAttack) {
            case 'STRIKE':
                if (userAnswer == "STRIKE") {
                    console.log("You have successfully defeated the Keeper of Shadows and saved your world!");
                    console.log("The Sword of Light barely survived the attack, unfortunately. ");
                    console.log("We will return the sword back to it's home in Purgatory.");
                    console.log("Great job, player. You have done well in this battle.");
                    console.log("The darkness has subsided...for now.");
                }
                //break?
            case 'IMPRISON':
                else
            {
                console.log("You have successfully defeated the Keeper of Shadows and saved your world!");
                console.log("The Sword of Light barely survived the attack, unfortunately. ");
                console.log("We will return the sword back to it's home in Purgatory.");
                console.log("Great job, player. You have done well in this battle.");
                console.log("The darkness has subsided...for now.");
            }
                break;
            default:
                console.log("Great job, player. You have done well in this battle.");
        }




            //The code is finished but the if/else statements were not able to be adjusted. 

