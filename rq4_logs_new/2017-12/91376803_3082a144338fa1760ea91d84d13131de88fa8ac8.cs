using System;
using System.Collections.Generic;
using System.Text;


namespace chuck_a_luck
{
    class Program
    {
        static void Main(string[] args)
        {
            new Game().Start();
        }

        public static void ExitProgram()
        {
            Console.ReadLine();
            Environment.Exit(1);
        }
    }

    class Game
    {
        private const int NumberThrows = 3;
        private List<Player> players;
        private Dice dice;
        private IEnumerable<int> NumbersDice
        {
            get
            {
                for (int i = 0; i < NumberThrows; i++)
                {
                    yield return dice.Roll();
                }
            }
        }

        public Game()
        {
            dice = new Dice();
        }

        public void Start()
        {
            Intro();
            EnterNames();
            GameLoop();
        }

        private void Intro()
        {
            Console.WriteLine("**** Chuck-a-luck ****\nSie haben 1000 Geldeinheiten\nIn jeder Runde können Sie einen Teil davon auf eine der Zahlen 1 bis 6 setzen. Danach werden 3 Würfel geworfen. Falls Ihr Wert dabei ist, erhalten Sie Ihren Einsatz zurück und zusatzlich Ihren Einsatz fuer jeden Würfel,der die von Ihnen gesetzte Zahl zeigt.");
        }

        private void EnterNames()
        {
            players = new List<Player>();
            string name;
            int i = 0;
            Player player;
            while (true)
            {
                i++;
                Console.Write($"Name Player {i}: ");
                name = Console.ReadLine();
                if (string.IsNullOrEmpty(name))
                    return;
                player = new Player(name);
                Subscribe(player);
                players.Add(player);
            }
        }

        private void GameLoop()
        {
            while (true)
            {
                GetBets();               
                CalcRound();
            }
        }

        private void GetBets()
        {
            for (int i = players.Count - 1; i >= 0; i--)
            {
                players[i].StartRound();
                players[i].InputBet();                
                players[i].InputNumber();
            }
        }

        private void CalcRound()
        {
            for (int i = players.Count - 1; i >= 0; i--)
            {
                players[i].CalcResult(NumbersDice);
                players[i].OutputResult();
                players[i].SaveRound();               
            }
        }
       
        private void GameOver()
        {
            Console.WriteLine("No Player left, leaving Game\n");
            Program.ExitProgram();
        }    

        private void RemovePlayer(Player player)
        {
            players.Remove(player);           
        }

        public void Subscribe(Player player)
        {
            player.PlayerQuits += QuitsHandler;
        }
        private void QuitsHandler(Player player)
        {
            RemovePlayer(player);
            if (players.Count == 0)
            {
                GameOver();
            }
        }
    }

    public class Player
    {
        private string Name;
        private int Money;
        private int MoneyBegin;
        private int Number;
        private int Result;
        private List<Round> RoundResults;
        private int Bet;

        public event PlayerQuitsHandler PlayerQuits;
        public delegate void PlayerQuitsHandler(Player player);

        public Player(string name)
        {
            Name = name;
            Money = 100;
            RoundResults = new List<Round>();
        }

        public void SaveRound()
        {
            RoundResults.Add(new Round(MoneyBegin, Money, Bet, Number, Result));
        }      

        public void OutputResult()
        {
            if (Result > 0)
                Console.WriteLine($"Glückwunsch {Name}, Sie erhalten {Result} Geldeinheiten!!");
            else
                Console.WriteLine($"Pech {Name}, da war nichts für Sie dabei!!, sie verlieren {Bet} Geldeinheiten");
        }

        public void StartRound()
        {
            Console.WriteLine($"{Name}'s turn ! ");
            Result = 0;
            MoneyBegin = Money;
            Console.WriteLine($"Sie haben {Money} Geldeinheiten");
        }

        public void InputBet()
        {
            Console.Write("Ihr Einsatz: ");
            string line = Console.ReadLine();
            Bet = Int32.Parse(line);
            if (Bet == 0)
            {
                Console.WriteLine($"{Name} verläßt das Casino mit {Money} Geldeinheiten!!");
                PlayerQuits(this);
            }
        }

        public void CalcResult(IEnumerable<int> numbersDice)
        {
            StringBuilder sb = new StringBuilder("");
            foreach (int numberDice in numbersDice)
            {
                sb.Append($" {numberDice}");
                if (Number == numberDice) Result += Bet;
            }
            if (Result > 0)
                Money += Result;
            else
                Money -= Bet;
            Console.Write($"Die Würfel sind gefallen: ");
            Console.WriteLine(sb.ToString());
            if (Money == 0)
            {
                Console.WriteLine($"Sorry {Name}, sie haben kein Geld mehr und sind raus !!");
                PlayerQuits(this);
            }
        }

        public void InputNumber()
        {
            Console.Write("Ihre Zahl: ");
            string line = Console.ReadLine();
            Number = Int32.Parse(line);
        }

        public struct Round
        {
            public int MoneyBegin, MoneyEnd, Number, Result, Bet;

            public Round(int moneyBegin, int moneyEnd, int bet, int num, int result)
            {
                MoneyBegin = moneyBegin;
                MoneyEnd = moneyEnd;
                Number = num;
                Result = result;
                Bet = bet;
            }
        }
    }

    class Dice
    {
        private Random rnd;
        private const int Min = 0;
        private const int Max = 6;

        public Dice()
        {
            rnd = new Random();
        }

        public int Roll()
        {
            return rnd.Next(Min, Max);
        }
    }
}