using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ClearantChallenge
{
    public class Testing
    {
        //Without a user interface, this seemed the best way to create data for testing
        [Fact]
        public void TestOne()
        {
            CreditCard One = CreateCreditCard.NewCard("Visa", 100);
            CreditCard Two = CreateCreditCard.NewCard("Mastercard", 100);
            CreditCard Three = CreateCreditCard.NewCard("Discover", 100);
            CreditCard[] TestOneCards = { One, Two, Three };
            Wallet Wallet1 = CreateNewWallet.CreateWallet("Wallet", TestOneCards);
            Wallet[] WalArray = { Wallet1 };
            Person Bob = CreateNewPerson.CreatePerson("Bob", WalArray);

            Assert.Equal(One.GetInterest(), 100m * .1m);
            Assert.Equal(Two.GetInterest(), 100m * .05m);
            Assert.Equal(Three.GetInterest(), 100m * .01m);
            Assert.Equal(Bob.GetInterest(), 16m);
        }
        [Fact]
        public void TestTwo()
        {
            CreditCard One = CreateCreditCard.NewCard("Visa", 100);
            CreditCard Two = CreateCreditCard.NewCard("Discover", 100);
            CreditCard Three = CreateCreditCard.NewCard("Mastercard", 100);
            CreditCard[] Wal1 = { One, Two };
            CreditCard[] Wal2 = { Three };
            Wallet Wallet1 = CreateNewWallet.CreateWallet("Wallet1", Wal1);
            Wallet Wallet2 = CreateNewWallet.CreateWallet("Wallet2", Wal2);
            Wallet[] WalArray = { Wallet1, Wallet2 };
            Person Tim = CreateNewPerson.CreatePerson("Tim", WalArray);

            Assert.Equal(Tim.GetInterest(), 16m);
            Assert.Equal(Wallet1.GetInterest(), (100m * .1m) + (100m * .01m));
            Assert.Equal(Wallet2.GetInterest(), 100m * .05m);
        }
        [Fact]
        public void TestThree()
        {
            CreditCard One = CreateCreditCard.NewCard("Visa", 100);
            CreditCard Two = CreateCreditCard.NewCard("Mastercard", 100);
            CreditCard[] Wal1 = { One, Two };
            CreditCard[] Wal2 = { One, Two };
            Wallet Wallet1 = CreateNewWallet.CreateWallet("Wallet1", Wal1);
            Wallet Wallet2 = CreateNewWallet.CreateWallet("Wallet2", Wal2);
            Wallet[] WalArray1 = { Wallet1 };
            Wallet[] WalArray2 = { Wallet2 };
            Person Fred = CreateNewPerson.CreatePerson("Fred", WalArray1);
            Person Dave = CreateNewPerson.CreatePerson("Dave", WalArray2);

            Assert.Equal(Fred.GetInterest(), (100m * .1m) + (100m * .05m));
            Assert.Equal(Wallet1.GetInterest(), (100m * .1m) + (100m * .05m));
            Assert.Equal(Dave.GetInterest(), (100m * .1m) + (100m * .05m));
            Assert.Equal(Wallet2.GetInterest(), (100m * .1m) + (100m * .05m));
        }
    }

    class Program
    {
        public static void Main()
        {
            CreditCard One = CreateCreditCard.NewCard("Visa", 100);
            CreditCard Two = CreateCreditCard.NewCard("Mastercard", 100);
            CreditCard Three = CreateCreditCard.NewCard("Discover", 100);
            CreditCard[] TestOneCards = { One, Two, Three };
            Wallet Wallet1 = CreateNewWallet.CreateWallet("Wallet", TestOneCards);
            Wallet[] WalArray = { Wallet1 };
            Person Bob = CreateNewPerson.CreatePerson("Bob", WalArray);

            Console.WriteLine(Bob.GetInterest());
            Console.ReadLine();
        }
    }

    class CreditCard
    {
        //Generic Credit Card Object
        public string CardName { get; set; }
        public decimal Rate { get; set; }
        public decimal Balance { get; set; }

        public CreditCard(string name, decimal rate, decimal balance)
        {
            this.CardName = name;
            this.Rate = rate;
            this.Balance = balance;
        }
        //With the Single Responsibility Principle, I considered making this its own
        //class. However, I couldn't think of a scenario this could be on its own.
        public decimal GetInterest()
        {
            return this.Balance * this.Rate;
        }

    }

    class Wallet
    {
        //This class violates the Dependancy Inversion Principle, but I cannot
        //come up with a way to make a Wallet, as a container for Credit Cards
        //as an abstraction
        public CreditCard[] Cards { get; set; }
        public string Name { get; set; }

        public Wallet(string name, CreditCard[] cards)
        {
            this.Name = name;
            this.Cards = cards;
        }

        public decimal GetInterest()
        {
            decimal total = 0m;
            foreach(CreditCard card in this.Cards)
            {
                total = card.GetInterest() + total;
            }
            return total;
        }
    }

    class Person
    {
        //Same as with Wallets- if person in this scenario is a container for Wallets
        //I cannot find a way to treat them as an abstraction to avoid violating DIP. 
        public string Name { get; set; }
        public Wallet[] Wallets { get; set; }

        public Person(string name, Wallet[] wallets)
        {
            this.Name = name;
            this.Wallets = wallets;
        }

        public decimal GetInterest()
        {
            decimal total = 0;
            foreach(Wallet wallet in this.Wallets)
            {
                total = total + wallet.GetInterest();
            }
            return total;
        }
    }

    class CreateCreditCard
    {
        //Class just to house a method to create a new card. Separate to reduce input needs
        public static CreditCard NewCard(string type, decimal balance)
        {
            CreditCard newcard = new CreditCard(type, GetInterestRate.Rate(type), 
                balance);
            return newcard;
        }
    }

    class GetInterestRate
    {
        //Returns the interest rate for the appropriate card. Kept separate for 
        //Single Responsibility Principle
        //Ideally, I wouldn't hard code this with magic numbers
        //but use external table instead
        public static decimal Rate(string type)
        {
            if (type.Equals("Mastercard"))
            {
                return .05m;
            }
            else if(type.Equals("Discover"))
            {
                return .01m;
            }
            else
            {
                return .10m;
            }
        }
    }

    class CreateNewWallet
    {
        public static Wallet CreateWallet(string name, CreditCard[] cards)
        {
            Wallet newWallet = new Wallet(name, cards);
            return newWallet;
        }
    }

    class CreateNewPerson
    {
        public static Person CreatePerson(string name, Wallet[] wallets)
        {
            Person newPerson = new Person(name, wallets);
            return newPerson;
        }
    }



}