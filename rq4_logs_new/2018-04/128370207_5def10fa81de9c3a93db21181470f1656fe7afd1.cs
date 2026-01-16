using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Windows;

namespace FridgeWPF
{
    public class MySqlDataBase : OnlineDataBase //klasa umożliwiająca łączenie się z bazą danych mySql
                                                  //dziedziczy część metod po OnlineDataBase
                                                  //zawiera narzędzia umożliwiające wprowadzanie zmian w wybranej bazie danych
    {
        public override string ConnectionString { get; protected set; } //domyślne ustawienie bazy danych. możliwe do zmiany przez
                                                                        //metodę SetDatabase dziedziczoną po OnlineDatabase
                                            = @"Server=sql11.freesqldatabase.com;
                                                            Database=sql11227333;   
                                                            Uid=sql11227333;
                                                            Pwd=F48xDrZZcw;";
        
        public override void AddIngredientToDatabase(AbstractIngredient ingredient)     //metoda dodająca składniki do bazy danych
        {
            try
            {
                MySqlConnection LocalConnection = (MySqlConnection)Connection;//downcasting, w celu dostosowania odpowiednich metod
                MySqlCommand LocalCommand = (MySqlCommand)Command;//downcasting, w celu dostosowania odpowiednich metod

                string CommandString = $"INSERT INTO FridgeContent (Name, Amount, ExpiryDate) " + //polecenie dodania rzędu w tabeli
                                   $"VALUES(@Name, @Amount, @ExpiryDate)";
                using (LocalConnection = new MySqlConnection(ConnectionString))
                {
                    LocalConnection.Open(); // otwarcie połączenia
                    using (LocalCommand = new MySqlCommand(CommandString, LocalConnection)) //przy otwartym połączeniu wypełniane
                    {                                                                       //są poszczególne kolumny
                        LocalCommand.Parameters.AddWithValue("@Name", ShortName(ingredient.Name));
                        LocalCommand.Parameters.AddWithValue("@Amount", ingredient.Amount);
                        LocalCommand.Parameters.AddWithValue("@ExpiryDate", ingredient.ExpiryDate);
                        LocalCommand.ExecuteNonQuery();
                    }
                    LocalConnection.Close();//zamyka połączenie
                }
            }
            catch (InvalidOperationException ex) { MessageBox.Show(ex.Message, "FreeSQL.AddIngredient"); }
            catch (MySqlException ex) { MessageBox.Show(ex.Message, "FreeSQL.AddIngredient"); }
            catch(Exception ex) { MessageBox.Show(ex.Message, "FreeSQL.AddIngredient"); }
        }

        public override void DeleteIngredientFromDatabase(AbstractIngredient ingredient)//metoda usuwająca wybrany składnik 
                                                                                        //z bazy danych
        {
            MySqlConnection LocalConnection = (MySqlConnection)Connection;//downcasting, w celu dostosowania odpowiednich metod
            MySqlCommand LocalCommand = (MySqlCommand)Command;//downcasting, w celu dostosowania odpowiednich metod

            string amountWithaDot = CommaFormat(ingredient.Amount.ToString());//zmienia format doubla dostosowując go do standardu
                                                                           //bazy danych, wykorzystując lokalną metodę CommaFormat            
            string CommandString = $"DELETE FROM FridgeContent " +                      //polecenie usunięcia składnika o podanych
                                    $"WHERE Name = '{ShortName(ingredient.Name)}' " +   //parametrach
                                    $"AND Amount = '{amountWithaDot}' " +
                                    $"AND ExpiryDate = '{ingredient.ExpiryDate}' " +
                                    $"LIMIT 1";

            using(LocalConnection = new MySqlConnection(ConnectionString))
            {
                LocalConnection.Open();//otwiera połączenie
                LocalCommand = new MySqlCommand(CommandString, LocalConnection);
                LocalCommand.ExecuteNonQuery();//wykonuje polecenie
                LocalConnection.Close();//zamyka połączenie
            }
        }

        string ShortName(string longName) //metoda skracająca nazwę składnika do pojedyńczego wyrazu, żeby móc ją porównywać w bazie
        {
            string shortName; //pole na skróconą nazwę

            string[] dividedName = longName.Split('.');//dzieli nazwę na pojedyńcze słowa

            shortName = dividedName[(dividedName.Length - 1)];//wykorzystuje jako nazwę tylko ostatnie słowo nazwy

            return shortName; //zwraca skróconą nazwę
        }

        private string CommaFormat(string input) //zmienia kropkę na przecinek, żeby ujednolicić format doubla z bazy i VS.
        {
            string output = "";
            for (int i = 0; i < input.Length; i++)
            {
                if (input[i] == ',')
                {
                    output += '.';
                }
                else
                {
                    output += input[i];
                }
            }
            return output;
        }

        public override void AddRecipeToDatabase(AbstractRecipe recipe) //zapisuje nowy przepis w bazie danych
        {
            try
            {
                MySqlConnection LocalConnection = (MySqlConnection)Connection;//downcasting, w celu dostosowania odpowiednich metod
                MySqlCommand LocalCommand = (MySqlCommand)Command;//downcasting, w celu dostosowania odpowiednich metod

                string CommandString = $"INSERT INTO Recipes (Name, Ingredients, Description) " + //polecenie dodania rzędu w tabeli
                                   $"VALUES(@Name, @Ingredients, @Description)";
                using (LocalConnection = new MySqlConnection(ConnectionString))
                {
                    LocalConnection.Open();//otwiera połączenie
                    using (LocalCommand = new MySqlCommand(CommandString, LocalConnection))
                    {                                   //wypełnia kolumny wartościami z przepisu przekazanego metodzie
                        LocalCommand.Parameters.AddWithValue("@Name", ShortName(recipe.Name));
                        LocalCommand.Parameters.AddWithValue("@Ingredients", ConvertToString(recipe));
                        LocalCommand.Parameters.AddWithValue("@Description", recipe.Description);
                        LocalCommand.ExecuteNonQuery();
                    }
                    LocalConnection.Close();//zamyka połączenie
                }
            }
            catch (InvalidOperationException ex) { MessageBox.Show(ex.Message); }
            catch (MySqlException) { MessageBox.Show($"Adding the new recipe failed. \n" +
                $"Maybe you already have recipe called {recipe.Name}?"); }
            catch (Exception ex) { MessageBox.Show(ex.Message); }
        }

        public string ConvertToString(AbstractRecipe recipe) // lokalna metoda zwracająca listę składników w postaci stringa,
        {                                                   //żeby ułatwić jego zapisanie w bazie danych
            string output = "";//pole na docelowy string
            Converter = new RecipeConverter();//zawiera narzędzia służące konwertowaniu typu Recipe na string i na odwrót
            for (int i = 0; i < recipe.ListOfIngredients.Count; i++)
            {
                output += Converter.FromIngredientToString(recipe.ListOfIngredients[i]);//dodaje kolejne nazwy i ilości do "output"
                if (i < recipe.ListOfIngredients.Count - 1)
                {
                    output += ";";//rozdziela kolejne składniki średnikiem
                }
            }
            return output;
        }
    }
}