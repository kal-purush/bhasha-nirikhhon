namespace GoF.CasinoCraps.ConsoleApp
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Contracts;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Wrapper for a craps game played through a console.
    /// </summary>
    public class ConsoleGame
    {
        private readonly IGame game;
        private readonly Player player;
        private readonly Action<string> writeOutput;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConsoleGame"/> class.
        /// </summary>
        /// <param name="game">The craps game object.</param>
        public ConsoleGame(IGame game, Action<string> writeOutput)
        {
            this.writeOutput = writeOutput;
            this.game = game;
            player = new Player(5000);
            player.JoinGame(game);
        }

        /// <summary>
        /// Gets the current round number for the game.
        /// </summary>
        public int RoundNumber
        {
            get
            {
                return game.RoundNumber;
            }
        }

        /// <summary>
        /// Executes the given command.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        /// <returns>A message with the results of the command.</returns>
        public void Execute(string command)
        {
            Contract.Requires(string.IsNullOrWhiteSpace(command) == false);

            if (command == "new-game")
            {
                game.Restart();
                writeOutput("new game started...");
                return;
            }

            string[] items = command.Split(' ');

            if (items[0] == "roll")
            {
                RollDice(items);

                foreach (var bet in game.CompletedBets)
                {
                    writeOutput(string.Format("Bet Completed: #{0}, Name: {1}, Status: {2}, Payout: ${3}", bet.Id, typeof(Bet).Name, Enum.GetName(typeof(BetStatus), bet.Status), bet.PayoutAmount));
                }
                return;
            }
            else if (items[0] == "place-bet")
            {
                PlaceBet(items);
                return;
            }
            else if (items[0] == "current-money")
            {
                writeOutput(string.Format("Current money is ${0}", player.Money));
                return;
            }

            writeOutput("unknown command");
        }

        private void RollDice(string[] items)
        {
            int currentRollNumber = game.RollNumber;
            Roll roll;
            if (items.Count() == 3)
            {
                roll = player.RollDice(Convert.ToInt32(items[1]), Convert.ToInt32(items[2]));
            }
            else
            {
                roll = player.RollDice();
            }

            writeOutput(string.Format("roll #{0} - [{1}] [{2}] - ({3})", currentRollNumber, roll.FirstDie, roll.SecondDie, roll.DiceTotal));
        }

        private void PlaceBet(string[] items)
        {
            Bet bet;

            string name = items[1];
            int amount = Convert.ToInt32(items[2]);

            switch (name)
            {
                case "snake-eyes":
                    bet = new SnakeEyesBet(amount);
                    break;
                case "ace-deuce":
                    bet = new AceDeuceBet(amount);
                    break;
                case "yo":
                    bet = new YoBet(amount);
                    break;
                case "boxcars":
                    bet = new BoxcarsBet(amount);
                    break;
                case "hi-lo":
                    bet = new HiLoBet(amount);
                    break;
                case "any-craps":
                    bet = new AnyCrapsBet(amount);
                    break;
                case "c-and-e":
                    bet = new CAndEBet(amount);
                    break;
                case "any-seven":
                    bet = new AnySevenBet(amount);
                    break;
                case "pass-line":
                    bet = new PassLineBet(amount);
                    break;
                case "dont-pass":
                    bet = new PassLineBet(amount);
                    break;
                default:
                    throw new ArgumentException("Bet name");
            }

            player.PlaceBet(bet);

            writeOutput(string.Format("Bet Placed: #{0}, Name: {1}, Status: {2}, Amount: ${3}", bet.Id, typeof(Bet).Name, Enum.GetName(typeof(BetStatus), bet.Status), bet.Amount));
        }
    }
}