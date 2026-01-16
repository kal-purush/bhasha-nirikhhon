using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kozel.Trumpnesses {
    public class NoTrumpness : TrumpnessBase {

        public override string Name {
            get {
                return "No trumpness";
            }
        }

        public NoTrumpness(Queue<Card> deck, List<Player> players) : base(deck, players) { }

        protected override void TrumpPlayer(Player player) {
            SetTrumpCards(CardSuit.Diamond);
        }
    }
}