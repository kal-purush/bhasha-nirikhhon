using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kozel {
    public class Game {
        private List<Player> players;

        public Game(List<Player> players) {
            this.players = players;
        }

        public void Start() {
            Queue<Card> deck = new Queue<Card>(32);
            FillDeck(deck);

            DealCards(deck, this.players);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="deck"></param>
        /// <param name="players"></param>
        private void DealCards(Queue<Card> deck, List<Player> players) {
            for (int i = 0; i < 8; i++) {
                for (int j = 0; j < 4; j++) {
                    players[j].AddCard(deck.Dequeue());
                }
            }
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="deck"></param>
        private void FillDeck(Queue<Card> deck) {
            List<Card> cardList = new List<Card> {
                new Card(CardSuit.Diamond, CardValue.Six), new Card(CardSuit.Heart, CardValue.Six), new Card(CardSuit.Spade, CardValue.Six), new Card(CardSuit.Club, CardValue.Six),
                new Card(CardSuit.Diamond, CardValue.Eight), new Card(CardSuit.Heart, CardValue.Eight), new Card(CardSuit.Spade, CardValue.Eight), new Card(CardSuit.Club, CardValue.Eight),
                new Card(CardSuit.Diamond, CardValue.Nine), new Card(CardSuit.Heart, CardValue.Nine), new Card(CardSuit.Spade, CardValue.Nine), new Card(CardSuit.Club, CardValue.Nine),
                new Card(CardSuit.Diamond, CardValue.King), new Card(CardSuit.Heart, CardValue.King), new Card(CardSuit.Spade, CardValue.King), new Card(CardSuit.Club, CardValue.King),
                new Card(CardSuit.Diamond, CardValue.Ten), new Card(CardSuit.Heart, CardValue.Ten), new Card(CardSuit.Spade, CardValue.Ten), new Card(CardSuit.Club, CardValue.Ten),
                new Card(CardSuit.Diamond, CardValue.Ace), new Card(CardSuit.Heart, CardValue.Ace), new Card(CardSuit.Spade, CardValue.Ace), new Card(CardSuit.Club, CardValue.Ace),
                new Card(CardSuit.Diamond, CardValue.Jack), new Card(CardSuit.Heart, CardValue.Jack), new Card(CardSuit.Spade, CardValue.Jack), new Card(CardSuit.Club, CardValue.Jack),
                new Card(CardSuit.Diamond, CardValue.Queen), new Card(CardSuit.Heart, CardValue.Queen), new Card(CardSuit.Spade, CardValue.Queen), new Card(CardSuit.Club, CardValue.Queen),
            };

            Random rnd = new Random();

            while (cardList.Count > 0) {
                int index = rnd.Next(cardList.Count - 1);
                deck.Enqueue(cardList[index]);
                cardList.RemoveAt(index);
            }

        }

    }
}