using System;
using Microsoft.SolverFoundation.Services;

namespace fruitshop
{
    class Program
    {
        static void Main(string[] args)
        {


            //Define Cost price of fruits
            double costApple = 10;
            double costPear = 15;
            double costBanana = 25;

            //Define Sales price of fruits
            double priceApple = 30;
            double pricePear = 30;
            double priceBanana = 60;

            //Define weight of fruits
            double weightApple = 50;
            double weightPear = 20;
            double weightBanana = 80;

            //Maximal budget
            double budget = 200;

            //Maximal weight
            double weightCapacity = 500;


            //Initiate/Create Solver
            var solver = SolverContext.GetContext();
            var model = solver.CreateModel();

            //Define variable which should be solved - result
            var decisionApple = new Decision(Domain.IntegerNonnegative, "AmountApples");
            var decisionPear = new Decision(Domain.IntegerNonnegative, "AmountPears");
            var decisionBanana = new Decision(Domain.IntegerRange(1, 100), "AmountBananas");

            //Add variables to the model
            model.AddDecision(decisionApple);
            model.AddDecision(decisionPear);
            model.AddDecision(decisionBanana);

            //Define what should be optimized - we want maximal profit
            model.AddGoal("GoalCost", GoalKind.Maximize,
                (priceApple - costApple) * decisionApple +
                (pricePear - costPear) * decisionPear +
                (priceBanana - costBanana) * decisionBanana
            );

            /*model.AddGoal("GoalWeight", GoalKind.Maximize,
                weightApple * decisionApple +
                weightPear * decisionPear +
                weightBanana * decisionBanana
            );*/

            //Add constraints
            //Maximum budget constraint
            model.AddConstraint("Budget",
                costApple * decisionApple +
                costPear * decisionPear +
                costBanana * decisionBanana
                <= budget
            );

            //Maximum weight capacity constraint
            model.AddConstraint("Weight",
                weightApple * decisionApple +
                weightPear * decisionPear +
                weightBanana * decisionBanana
                <= weightCapacity
            );

            var solution = solver.Solve();

            double totalweight = decisionApple.GetDouble() * weightApple + decisionPear.GetDouble() * weightPear + decisionBanana.GetDouble() * weightBanana;
            double totalCost = decisionApple.GetDouble() * costApple + decisionPear.GetDouble() * costPear + decisionBanana.GetDouble() * costBanana;

            Console.WriteLine("Apple = " + decisionApple.GetDouble());
            Console.WriteLine("Pear = " + decisionPear.GetDouble());
            Console.WriteLine("Banana = " + decisionBanana.GetDouble());
            Console.WriteLine("__________________");

            Console.WriteLine("Total Weight = {0} ", totalweight);
            Console.WriteLine("Total Cost = {0} ", totalCost);

            Console.WriteLine("__________________");
            Console.WriteLine("Solution quality: " + solution.Quality);

            Console.ReadLine();


        } // Main
    } // Program
} // ns