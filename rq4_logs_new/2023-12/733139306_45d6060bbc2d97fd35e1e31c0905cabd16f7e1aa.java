package lesson15.lessoncode.service;


import lesson15.lessoncode.entity.Cards;
import lesson15.lessoncode.entity.Player;
import lesson15.lessoncode.service.utils.CardTableUtil;
import lesson15.lessoncode.service.utils.DeckService;
import lesson15.lessoncode.service.utils.InputData;
import lesson15.lessoncode.service.utils.PrintServices;

public class CardTable {
   Cards cards = new Cards();

   DeckService deckService = new DeckService();
   CardTableUtil cardTableUtil= new CardTableUtil();
   PrintServices printServices = new PrintServices();

    InputData iData = new InputData();
    public int numberOfPayers = iData.enterNumberOfPlayers();
    public Player[] players = new Player[numberOfPayers];

    int numberCardsForEachPlayer = 5;

public void game(){

    deckService.fillDeck(cards.getDeck());
    printServices.printDeck(cards.getDeck(), "----------- Create New Deck -----------");

    cards.setDeckShuffle(deckService.deckShuffle(cards.getDeck()));

    printServices.printDeck(cards.getDeckShuffle(), "--------- Deck after shuffle --------");

    cardTableUtil.createPlayers(players);
    cardTableUtil.dealCards(cards.getDeckShuffle(), numberCardsForEachPlayer, numberOfPayers, players);
    printServices.printPlayers(players);



}


}