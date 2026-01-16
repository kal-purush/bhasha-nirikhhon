function seq(max, min) {
    return Math.floor(Math.random() * (max - min + 1) + min)
}
//player and PC choices function
function choice(play){
    let result = ""
    if(play == 1)
    {
        result = "chooses: rock 🥌"
    }
    else if(play == 2)
    {
        result = "chooses: paper 🧻"
    }
    else if(play == 3)
    {
        result = "chooses: scissors ✂️"
    }
    else
    {
        result = "¿bro? 🤨"
    }
    return result
}
let player = 0
let PC = 0
let victories = 0
let losses = 0
//cycle works to interate over each round
while(victories < 3 && losses < 3)
{
    PC = seq(3, 1)
    player = prompt("Choose a number:\n1)Rock 🥌\n2)Paper 🧻\n3)Scissors ✂️")
    alert("PC " + choice(PC))
    alert("player " + choice(player))
    //FIGHT!!
    if(PC == player)
    {
        alert("IT'S A TIE!")
    }
    else if(player == 1 && PC == 3)
    {
        alert("YOU'VE WON!")
        victories = victories+1
    }
    else if(player == 2 && PC == 1)
    {
        alert("YOU'VE WON!")
        victories = victories+1
    }
    else if(player == 3 && PC == 2)
    {
        alert("YOU'VE WON!")
        victories = victories+1
    }
    else
    {
        alert("YOU'VE LOST!")
        losses = losses+1
    }
}
if(victories > losses)
{
    alert("YOU'VE WON! " + "\nlosses: " + losses + "\nvictories: " + victories)
}
else
{
    alert("YOU'VE LOST! " + "\nlosses: " + losses + "\nvictories: " + victories)
}
//1 = Rock
//2 = Paper
//3 = Scissors