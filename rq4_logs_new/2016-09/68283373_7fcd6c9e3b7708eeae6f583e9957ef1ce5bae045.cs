using System;
using System.Collections.Generic;
using System.Data;

// Let there be a code class!!
public class HaBier
{
	// Variables

	public static void Main(string[] args)
	{
		DataTable table = GetTable();
		DataTable fris = SetTable();

		Console.Write("\n\nWelcome at Dix Store. :\n");
		Console.Write("----------------------------------------------------------------\n");

		//input velden
		Console.Write("Please input a name: ");
		string name = Console.ReadLine();
		Console.Write("Please input Age: ");
		int age = Convert.ToInt32(Console.ReadLine());

		Console.WriteLine("\n");
		verifyAge(name, age);
	}


	/*
     * FUNCTIONS
     */
	static DataTable GetTable()
	{
		// Here we create a DataTable with 2 columns.
		DataTable table = new DataTable();
		table.Columns.Add("Brand", typeof(string));
		table.Columns.Add("Price", typeof(decimal));

		// Here we add 2 DataRows.
		table.Rows.Add("Amstel", 3.95);
		table.Rows.Add("Heineken", 10.50);
		table.Rows.Add("Grolsch", 0.69);
		table.Rows.Add("Bavaria", 13.37);
		table.Rows.Add("Palm", 420);
		table.Rows.Add("Bok", 0.0101);
		return table;
	}
	static DataTable SetTable()
	{
		// Here we create a DataTable with 2 columns.
		DataTable fris = new DataTable();
		fris.Columns.Add("Merk", typeof(string));
		fris.Columns.Add("Prijs", typeof(decimal));

		// Here we add 2 DataRows.
		fris.Rows.Add("Cola", 3.95);
		fris.Rows.Add("Fanta", 10.50);
		fris.Rows.Add("fristi", 0.69);
		return fris;
	}




	// Age verifier
	private static void verifyAge(string name, int age)
	{
		DataTable data = GetTable();
		string selectedBrand;
		int amount;

		DataTable fris = SetTable();
		string SelectedMerk;
		int aantal;

		//if statement voor leeftijd check
		if (age >= 18)
		{
			Console.WriteLine("Welcome to the Club Mister " + name + " !");
			Console.WriteLine("You are " + age + " years old!");
			Console.WriteLine("\n" + "Do you want to see the available menu 'Y/n");



			//
			switch (Console.ReadLine())
			{
				case "n":
					break;
				default:
					Console.WriteLine("\n" + "Do you want to see beer/fris menu 'f/n");

					switch (Console.ReadLine())
					{
						case "b":
							foreach (DataRow row in data.Rows)
							{
								Console.WriteLine(row.Field<string>(0));
							}

							Console.Write("\n" + "what beer do you like? ");
							selectedBrand = Console.ReadLine();
							Console.WriteLine("Enjoy your: " + selectedBrand + "!");

							//hoeveelheid bier
							Console.Write("\nHow many would you like? ");
							amount = Convert.ToInt32(Console.ReadLine());

							string query = string.Format("Brand = '{0}'", selectedBrand);
							DataRow[] foundRows = data.Select(query);
							foreach (DataRow row in foundRows)
							{
								Console.WriteLine("\nMakes a total of:" + (decimal) row[1] * amount);
							}

							Console.ReadLine();
							break;

						case "f":
							foreach (DataRow row in fris.Rows)
							{
								Console.WriteLine(row.Field<string>(0));
							}

							Console.Write("\n" + "what fris do you like? ");
							SelectedMerk = Console.ReadLine();
							Console.WriteLine("Enjoy your: " + SelectedMerk + "!");

							//hoeveelheid bier
							Console.Write("\nHow many would you like? ");
							aantal = Convert.ToInt32(Console.ReadLine());

							string queery = string.Format("Brand = '{0}'", SelectedMerk);
							DataRow[] foundrows = data.Select(queery);
							foreach (DataRow row in foundrows)
							{
								Console.WriteLine("\nMakes a total of:" + (decimal) row[1] * aantal);
							}

							Console.ReadLine();
							break;
					}
					break;

			}
		}
	




		else
		{
			Console.WriteLine("Welcome Mister " + name + " !");
			Console.WriteLine("You are " + age + " years old!");
			Console.WriteLine("The Age of " + age + " is too young to enter!");

			Console.ReadLine();
		}
    }
}