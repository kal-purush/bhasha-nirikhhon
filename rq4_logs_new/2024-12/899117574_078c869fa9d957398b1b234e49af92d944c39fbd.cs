using Spectre.Console;
using Spectre.Console.Cli;
using MortgageCalculator;
namespace SpectreUI

{
	internal class MortgageSpectreConsole
	{
		

		public struct UserInformation
		{
			//public string firstName;
			//public string lastName;
			//public string email;

			//public decimal loanAmount;
			//public int term;
			//public decimal interestRate;


			public decimal LoanAmount { get; set; }
			public decimal Term { get; set; }
			public decimal InterestRate { get; set; }

		}
		static void Main(string[] args)
		{
			UserInformation userInformation = new UserInformation();

			AnsiConsole.Write(new FigletText("Mortgage Calculator").Color(Color.DarkRed).Centered());

			var selcetion = AnsiConsole.Prompt(
				new MultiSelectionPrompt<string>()
					.Title("What are your [green]favorite fruits[/]?")
					.NotRequired() 
					.PageSize(10)
					.AddChoices(new[] {
			"Apple", "Apricot", "Avocado",
			"Banana", "Blackcurrant", "Blueberry",
			"Cherry", "Cloudberry", "Coconut",
		}));

			userInformation.LoanAmount = AnsiConsole.Prompt(
					new TextPrompt<decimal>("Loan Amount: ")
						.Validate((n) => n != 0 ?
						ValidationResult.Success() :
						ValidationResult.Error("The loan amount must be greater than zero")));
			userInformation.Term = AnsiConsole.Prompt(
					new TextPrompt<int>("Term of the Loan: ")
						.Validate((n) => n != 0 ?
						ValidationResult.Success() :
						ValidationResult.Error("The term of the loan must be greater than zero")));

			userInformation.InterestRate = AnsiConsole.Prompt(
					new TextPrompt<decimal>("Interest Rate: ")
						.Validate((n) => n != 0 ?
						ValidationResult.Success() :
						ValidationResult.Error("The Loan amount must be greate than zero")));

			//MortgageCalculator.MortgageCalc mortgageCalc = new();
				
			//switch statement 



			//define global variables

			// Create the layout
			//var layout = new Layout("Root")
			//	.SplitRows(
			//		new Layout("Top").SplitColumns(
			//			new Layout("Left"),
			//			new Layout("Right")),
			//		new Layout("Bottom"));


			//// Update the left column
			//layout["Left"].Update(
			//	new Panel(
			//		Align.Center(
			//			new Markup("Hello [blue]World![/]"),
			//			VerticalAlignment.Middle))
			//		.Expand());

			var confirmation = AnsiConsole.Prompt(
	new TextPrompt<bool>("Run prompt example?")
		.AddChoice(true)
		.AddChoice(false)
		.DefaultValue(true)
		.WithConverter(choice => choice ? "y" : "n"));

			//layout["Top"]["Left"].Update(AnsiConsole.Prompt(
			//		userInformation.loanAmount = new TextPrompt<decimal>("Loan Amount: ").Validate((n) => n != 0 ? ValidationResult.Success() : ValidationResult.Error("The Loan amount must be greate than zero")))

			// render the layout
			//AnsiConsole.Write(layout);
		}
	}
}