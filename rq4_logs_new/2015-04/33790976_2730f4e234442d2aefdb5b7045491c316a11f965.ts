/**
 * Created by dhnishi on 4/11/15.
 */
/// <reference path="../../typings/jasmine/jasmine.d.ts"/>

/// <reference path="../scripts/Card.ts"/>
/// <reference path="../scripts/Hand.ts"/>
/// <reference path="../scripts/HandEvaluator.ts"/>


describe("Hand Evaluator", () => {
    it("Evaluate non-ace hands.", () => {
        var firstCard = new Card(Suit.CLUBS, Rank.EIGHT);
        var secondCard = new Card(Suit.DIAMONDS, Rank.KING);
        var testHand = new Hand([firstCard, secondCard]);
        var handEvaluator = new HandEvaluator();
        expect(handEvaluator.evaluate(testHand)).toEqual(18);
    });

    it("Evaluate all face cards.", () => {
        var jack = new Card(Suit.DIAMONDS, Rank.JACK);
        var testHand = new Hand([jack]);
        var handEvaluator = new HandEvaluator();
        expect(handEvaluator.evaluate(testHand)).toEqual(10);

        var queen = new Card(Suit.DIAMONDS, Rank.QUEEN);
        expect(handEvaluator.evaluate(new Hand([queen]))).toEqual(10);

        var king = new Card(Suit.DIAMONDS, Rank.KING);
        expect(handEvaluator.evaluate(new Hand([king]))).toEqual(10);
    });

    it("Evaluate ace hands.", () => {
        var ace = new Card(Suit.CLUBS, Rank.ACE);
        var testHand = new Hand([ace]);
        var handEvaluator = new HandEvaluator();
        expect(handEvaluator.evaluate(testHand)).toEqual(11);

        testHand.add(new Card(Suit.CLUBS, Rank.JACK));
        expect(handEvaluator.evaluate(testHand)).toEqual(21);

        testHand.add(new Card(Suit.CLUBS, Rank.JACK));
        expect(handEvaluator.evaluate(testHand)).toEqual(21);

        testHand.add(new Card(Suit.CLUBS, Rank.JACK));
        expect(handEvaluator.evaluate(testHand)).toEqual(0);
    })
});