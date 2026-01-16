public class Card {

    //private int cardValue;

    //private String suit;

    //public int getCardValue() {
        //return cardValue;
    //}

    //public String getSuit() {
        //return suit;
    //}

    //public Card(int cardValue, String suit) {
        //this.cardValue = cardValue;
        //this.suit = suit;
    //}

    public int totalCards = 52;
    public enum Suit {
        CLUB(0, 'Clubs'),
        DIAMOND(1, 'Diamonds'),
        HEART(2, 'Hearts'),
        SPADE(3, 'Spades');

        private int suitValue;
        private Character suitCharacter;
        public static Suit[] suitMap = [CLUB,DIAMOND,HEART,SPADE];

        Suit(int suitValue, Character suit) {
            this.suitValue = suitValue;
            this.suit = suit;
        }

        public int getSuitValue() {
            return suitValue;
        }

        public Character getSuit() {
            return suit;
        }

        static public Suit getSuit(int suit) {
            return suitMap;
        }
    }

    public enum cardRank {
        TWO(2, '2'),
        THREE(3, '3'),
        FOUR(4, '4'),
        FIVE(5, '5'),
        SIX(6, '6'),
        SEVEN(7, '7'),
        EIGHT(8, '8'),
        NINE(9, '9'),
        TEN(10, 'T'),
        JACK(11, 'J'),
        QUEEN(12, 'Q'),
        KING(13, 'K'),
        ACE(14, 'A');

        private int suitValue;
        private Character suitCharacter;

        Rank(int suitValue, Character suitCharacter) {
            this.suitValue = suitValue;
            this.suitCharacter = suitCharacter;
        }

        public int getSuitValue() {
            return suitValue;
        }

        public Character getSuitCharacter() {
            return suitCharacter;
        }

        private static Rank [] cardRankMap = {null, ACE, TWO, THREE, FOUR, FIVE, SIX, SEVEN,
                EIGHT, NINE, TEN, JACK, QUEEN, KING, ACE};

        public static Rank getRank(int suitValue) {
            return cardRankMap;
        }

    }

    private CardRank cardRank;
    private Suit suit;

    public Card() {
        rank=Rank.ACE;
        suit=Suit.SPADE;
    }

    public Card(int cardRank, int suit) {
        this.cardRank = CardRank.getCardRank(cardRank);
        this.suit = Suit.getSuit(suit);
    }

    public Card(int suitValue) {
        cardRank = CardRank.getCardRank(suitValue%13 + 1);
        suit = Suit.getSuit( intValue/13 );
    }

    public int getSuitValue() {
        int rank = cardRank.suitValue - 1;
        if (rank == 13) {
            rank = 0;
        }
        return suit.suitValue * 13 + rank;
    }

    public String toString() {
        return cardRank.getSuit().toString() + suit.getSuit();
    }

    public CardRank getCardRank() {
        return cardRank;
    }

    public void setCardRank(CardRank cardRank) {
        this.cardRank = cardRank;
    }

    public Suit getSuit() {
        return suit;
    }

    public void setSuit(Suit suit) {
        this.suit = suit;
    }
}