const imageDirectory = "cards-png-100x153-numbered";
//similar to let but cannot be changed
$(document).ready(function (){
    $("img#drawDeck").click(drawCard);
    $("button#discardAllButton").click(discardAllCards);
});
function drawCard(){
    let randomNumber = Math.floor(Math.random()*52)+1;
    let cardImageTag = $("<img>");
    let srcAddress = `${imageDirectory}/${randomNumber}`;
    cardImageTag.attr("src", srcAddress);
    cardImageTag.attr("alt", "playing card");
    $("div#playArea").append(cardImageTag);

}
function discardAllCards(){
    let lastCardDealt = $("div#playArea img:last-child");
    $("div#playArea img").remove();
    $("div#discardPile").append(lastCardDealt);
}