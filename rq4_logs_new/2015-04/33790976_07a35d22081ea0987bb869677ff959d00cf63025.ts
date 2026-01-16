/**
 * Created by dhnishi on 4/12/15.
 */

/// <reference path="../../../typings/angularjs/angular.d.ts"/>
/// <reference path="../Card.ts"/>
/// <reference path="../main.ts"/>

app.factory('deckService', makeDeckService);

function makeDeckService() {
    // TODO: Maybe turn this into its own class?
    var service : any = {};

    /**
     * Generates a new deck of cards in sorted order.
     */
    var buildDeck = () => {
        var deck = [];
        var TWO = Rank.TWO.valueOf();

        for (var suit in Suit) {
            for (var rank in Rank) {
                if (typeof Suit[suit] === "number" && typeof Rank[rank] === "number") {
                    // Ignore invalid cards.
                    if (parseInt(Rank[rank]) >= TWO) {
                        deck.push(new Card(Suit[suit], Rank[rank]));
                    }
                }
            }
        }
        return deck;
    };

    /**
     * Builds a shuffled deck of cards.
     * @returns {Card[]} A shuffled deck of cards.
     */
    service.buildDeck = () => {
        var deck : Card[] = buildDeck();
        var count = deck.length;

        while (count > 0) {
            // Select a random card.
            var index = Math.floor(Math.random() * count);
            count--;

            // Put it at the end.
            var temp = deck[count];
            deck[count] = deck[index];
            deck[index] = temp;
        }
        return deck;
    };

    return service;
}