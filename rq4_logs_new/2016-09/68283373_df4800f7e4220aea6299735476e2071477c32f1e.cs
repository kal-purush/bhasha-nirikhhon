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
        return table;
    }

    // Age verifier
    private static void verifyAge(string name, int age)
    {
        DataTable data = GetTable();
        string selectedBrand;
        int amount;

        //if statement voor leeftijd check
        if (age >= 18)
        {
            Console.WriteLine("Welcome Mister " + name + " !");
            Console.WriteLine("You are " + age + " years old!");
            Console.WriteLine("\n" + "Do you want to see the available brand? Y/n");

            switch (Console.ReadLine())
            {
                case "n":
                    break;
                default:
                    foreach (DataRow row in data.Rows)
                    {
                        Console.WriteLine(row.Field<string>(0));
                    }
                    break;
            }

            Console.Write("\n" + "what beer do you like? ");
            selectedBrand = Console.ReadLine();
            Console.WriteLine("Have a nice " + selectedBrand + "!");

            //hoeveelheid bier
            Console.Write("\nHow many would you like? ");
            amount = Convert.ToInt32(Console.ReadLine());

            string query = string.Format("Brand = '{0}'", selectedBrand);
            DataRow[] foundRows = data.Select(query);
            foreach (DataRow row in foundRows)
            {
                Console.WriteLine("\n" + (decimal)row[1] * amount);
            }

            Console.ReadLine();
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