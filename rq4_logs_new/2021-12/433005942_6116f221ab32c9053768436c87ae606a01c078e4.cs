using System;
using System.Data;
using System.Linq;

namespace Excel
{
    public class Logic
    {
        DataTable DataGrid = new DataTable();
        public string Rozpoznaj(DataTable dt, string a)
        {
            DataGrid = dt;
            string wynik = string.Empty;
            bool tak = true;
            if (a.StartsWith("=Suma(") && a.Contains(":") && a.EndsWith(")") && tak)
            {
                wynik = Suma(a);
                tak = false;
            }
            if (a.StartsWith("=Avg(") && a.Contains(":") && a.EndsWith(")") && tak)
            {
                wynik = AVG(a);
                tak = false;
            }
            if (a.StartsWith("=Pow(") && a.Contains(",") && a.EndsWith(")") && tak)
            {
                wynik = Pow(a);
                tak = false;
            }
            if (a.StartsWith("=Root(") && a.EndsWith(")") && tak)
            {
                wynik = Root(a);
                tak = false;
            }
            if (a.Contains("*") && tak)
            {
                wynik = Multiply(a);
                tak = false;
            }
            if (a.Contains("/") && tak)
            {
                wynik = Divide(a);
                tak = false;
            }
            if (a.Contains("-") && tak)
            {
                wynik = Substract(a);
                tak = false;
            }
            if (a.Contains("+") && tak)
            {
                wynik = Add(a);
            }

            return wynik;
        }

        public string Suma(string a)
        {
            double wynik = 0.0;

            string sum = a.Substring(6, a.Length - 7);
            var sp = sum.Split(":");

            int col1 = sp[0][0] - 65;
            int row1 = Convert.ToInt32(sp[0].Remove(0, 1)) - 1;
            int col2 = sp[1][0] - 65;
            int row2 = Convert.ToInt32(sp[1].Remove(0, 1)) - 1;

            if (col1 == col2)
            {
                if (row1 < row2)
                {
                    for (int i = row1; i <= row2; i++)
                    {
                        wynik += Convert.ToDouble(DataGrid.Rows[i][col1]);
                    }
                }
                else if (row1 > row2)
                {
                    for (int i = row2; i <= row1; i++)
                    {
                        wynik += Convert.ToDouble(DataGrid.Rows[i][col1]);
                    }
                }
                else
                {
                    wynik += Convert.ToDouble(DataGrid.Rows[row1][col1]);
                }
            }
            else if (col1 < col2)
            {
                if (row1 < row2)
                {
                    for (int j = col1; j <= col2; j++)
                    {
                        for (int i = row1; i <= row2; i++)
                        {
                            wynik += Convert.ToDouble(DataGrid.Rows[i][j]);
                        }
                    }
                }
                else if (row1 > row2)
                {
                    for (int j = col1; j <= col2; j++)
                    {
                        for (int i = row2; i <= row1; i++)
                        {
                            wynik += Convert.ToDouble(DataGrid.Rows[i][j]);
                        }
                    }
                }
                else
                {
                    for (int i = col1; i <= col2; i++)
                    {
                        wynik += Convert.ToDouble(DataGrid.Rows[row1][col1]);
                    }
                }
            }
            else
            {
                if (row1 < row2)
                {
                    for (int j = col2; j <= col1; j++)
                    {
                        for (int i = row1; i <= row2; i++)
                        {
                            wynik += Convert.ToDouble(DataGrid.Rows[i][j]);
                        }
                    }
                }
                else if (row1 > row2)
                {
                    for (int j = col2; j <= col1; j++)
                    {
                        for (int i = row2; i <= row1; i++)
                        {
                            wynik += Convert.ToDouble(DataGrid.Rows[i][j]);
                        }
                    }
                }
                else
                {
                    for (int i = col2; i <= col1; i++)
                    {
                        wynik += Convert.ToDouble(DataGrid.Rows[row1][col1]);
                    }
                }
            }
            return wynik.ToString();
        }

        public string AVG(string a)
        {
            double wynik = 0.0;
            int count = 1;

            string avg = a.Substring(5, a.Length - 6);
            var sp = avg.Split(":");

            int col1 = sp[0][0]-65;
            int row1 = Convert.ToInt32(sp[0].Remove(0, 1)) - 1;
            int col2 = sp[1][0]-65;
            int row2 = Convert.ToInt32(sp[1].Remove(0, 1)) - 1;
            if (col1 == col2)
            {
                if (row1 < row2)
                {
                    for (int i = row1; i <= row2; i++)
                    {
                        wynik += Convert.ToDouble(DataGrid.Rows[i][(char)col1].ToString());
                        count++;
                    }
                }
                else if (row1 > row2)
                {
                    for (int i = row2; i <= row1; i++)
                    {
                        wynik += Convert.ToDouble(DataGrid.Rows[i][(char)col1].ToString());
                        count++;
                    }
                }
                else
                {
                    wynik += Convert.ToDouble(DataGrid.Rows[row1][(char)col1].ToString());
                }
            }
            else if (col1 < col2)
            {
                if (row1 < row2)
                {
                    for (int j = col1; j <= col2; j++)
                    {
                        for (int i = row1; i <= row2; i++)
                        {
                            wynik += Convert.ToDouble(DataGrid.Rows[i][(char)j].ToString());
                            count++;
                        }
                    }
                }
                else if (row1 > row2)
                {
                    for (int j = col1; j <= col2; j++)
                    {
                        for (int i = row2; i <= row1; i++)
                        {
                            wynik += Convert.ToDouble(DataGrid.Rows[i][(char)j].ToString());
                            count++;
                        }
                    }
                }
                else
                {
                    for (int i = col1; i <= col2; i++)
                    {
                        wynik += Convert.ToDouble(DataGrid.Rows[row1][(char)col1].ToString());
                        count++;
                    }
                }
            }
            else
            {
                if (row1 < row2)
                {
                    for (int j = col2; j <= col1; j++)
                    {
                        for (int i = row1; i <= row2; i++)
                        {
                            wynik += Convert.ToDouble(DataGrid.Rows[i][(char)j].ToString());
                            count++;
                        }
                    }
                }
                else if (row1 > row2)
                {
                    for (int j = col2; j <= col1; j++)
                    {
                        for (int i = row2; i <= row1; i++)
                        {
                            wynik += Convert.ToDouble(DataGrid.Rows[i][(char)j].ToString());
                            count++;
                        }
                    }
                }
                else
                {
                    for (int i = col2; i <= col1; i++)
                    {
                        wynik += Convert.ToDouble(DataGrid.Rows[row1][(char)col1].ToString());
                        count++;
                    }
                }
            }
            wynik = wynik / count;
            return wynik.ToString();
        }

        public string Pow(string a)
        {
            long wynik = 0;
            int potega = 0;
            int podstawa;

            string avg = a.Substring(5, a.Length - 6);
            var sp = avg.Split(",");

            if (sp[0].All(char.IsDigit))
            {
                podstawa = Convert.ToInt32(sp[1]);
            }
            else
            {
                int col1 = sp[0][0] - 65;
                int row1 = Convert.ToInt32(sp[0].Remove(0, 1)) - 1;
                podstawa = Convert.ToInt32(DataGrid.Rows[row1][col1]);
            }
            if (sp[1].All(char.IsDigit))
            {
                potega = Convert.ToInt32(sp[1]);
            }
            else
            {
                int col2 = sp[1][0] - 65;
                int row2 = Convert.ToInt32(sp[1].Remove(0, 1)) - 1;
                potega = Convert.ToInt32(DataGrid.Rows[row2][col2]);
            }
            return Math.Pow(podstawa, potega).ToString();
        }

        public string Root(string a)
        {
            long wynik = 0;
            int podstawa;

            string avg = a.Substring(6, a.Length - 7);
            var sp = avg.Split(",");

            if (sp[0].All(char.IsDigit))
            {
                podstawa = Convert.ToInt32(sp[1]);
            }
            else
            {
                int col1 = sp[0][0] - 65;
                int row1 = Convert.ToInt32(sp[0].Remove(0, 1)) - 1;
                podstawa = Convert.ToInt32(DataGrid.Rows[row1][col1]);
            }
            return Math.Sqrt(podstawa).ToString();
        }

        public string Add(string a)
        {
            var sp = a.Remove(0, 1).Split("+");
            double wynik = 0.0;
            for (int i = 0; i < sp.Length; i++)
            {
                if (sp[i].All(char.IsDigit))
                {
                    wynik += Convert.ToDouble(sp[i]);
                }
                else if (sp[i].All(char.IsLetter))
                {
                    Console.WriteLine("kol");
                }
                else if (sp[i].Length > 1 && sp[i].Length < 4)
                {
                    int col = ((int)sp[i][0] - 65);
                    int row = Convert.ToInt32(sp[i].Remove(0, 1)) - 1;
                    if (DataGrid.Rows[row][col] != null)
                    {
                        wynik += Convert.ToDouble(DataGrid.Rows[row][col]);
                    }
                    else
                    {
                        wynik += 0;
                    }
                }
                else
                {
                    Console.WriteLine("Błąd");
                }
            }
            return wynik.ToString();
        }

        public string Substract(string a)
        {
            var sp = a.Remove(0, 1).Split("-");
            double wynik = 0.0;
            for (int i = 0; i < sp.Length; i++)
            {
                if (sp[i].All(char.IsDigit))
                {
                    if (a.Remove(0, 1)[0] == '-' && i == 0)
                    {
                        wynik -= Convert.ToDouble(sp[i + 1]);
                        i++;
                    }
                    else if (i == 0)
                    {
                        wynik += Convert.ToDouble(sp[i]);
                    }
                    else
                    {
                        wynik -= Convert.ToDouble(sp[i]);
                    }
                }
                else if (sp[i].All(char.IsLetter))
                {
                    Console.WriteLine("kol");
                }
                else if (sp[i].Length > 1 && sp[i].Length < 4)
                {
                    int col = ((int)sp[i][0] - 65);
                    int row = Convert.ToInt32(sp[i].Remove(0, 1)) - 1;
                    if (DataGrid.Rows[row][col] != null)
                    {
                        string cellV = DataGrid.Rows[row][col].ToString();
                        if (cellV.Length >= 2 && cellV.Remove(0, 1)[0] == '-' && i == 0)
                        {
                            wynik -= Convert.ToDouble(cellV);
                            i++;
                        }
                        else if (i == 0)
                        {
                            wynik += Convert.ToDouble(cellV);
                        }
                        else
                        {
                            wynik -= Convert.ToDouble(cellV);
                        }
                    }
                    else
                    {
                        wynik += 0;
                    }
                }
                else
                {
                    Console.WriteLine("Błąd");
                }
            }
            return wynik.ToString();
        }

        public string Multiply(string a)
        {
            var sp = a.Remove(0, 1).Split("*");
            decimal wynik = 1;
            for (int i = 0; i < sp.Length; i++)
            {
                if (sp[i].All(char.IsDigit))
                {
                    wynik *= Convert.ToDecimal(sp[i]);
                }
                else if (sp[i].All(char.IsLetter))
                {
                    Console.WriteLine("kol");
                }
                else if (sp[i].Length > 1 && sp[i].Length < 4)
                {
                    int col = ((int)sp[i][0] - 65);
                    int row = Convert.ToInt32(sp[i].Remove(0, 1)) - 1;
                    if (DataGrid.Rows[row][col] != null)
                    {
                        wynik *= Convert.ToDecimal(DataGrid.Rows[row][col]);
                    }
                    else
                    {
                        wynik *= 1;
                    }
                }
                else
                {
                    Console.WriteLine("Błąd");
                }
            }
            return wynik.ToString();
        }

        public string Divide(string a)
        {
            var sp = a.Remove(0, 1).Split("/");
            double wynik = 0.0;
            for (int i = 0; i < sp.Length; i++)
            {
                if (sp[i].All(char.IsDigit))
                {
                    if (i == 0)
                    {
                        wynik = Convert.ToDouble(sp[i]);
                    }
                    else
                    {
                        wynik /= Convert.ToDouble(sp[i]);
                    }
                }
                else if (sp[i].All(char.IsLetter))
                {
                    Console.WriteLine("kol");
                }
                else if (sp[i].Length > 1 && sp[i].Length < 4)
                {
                    int col = ((int)sp[i][0] - 65);
                    int row = Convert.ToInt32(sp[i].Remove(0, 1)) - 1;
                    if (DataGrid.Rows[row][col] != null)
                    {
                        if (i == 0)
                        {
                            wynik = Convert.ToDouble(DataGrid.Rows[row][col]);
                        }
                        else
                        {
                            wynik /= Convert.ToDouble(DataGrid.Rows[row][col]);
                        }
                    }
                    else
                    {
                        wynik += 0;
                    }
                }
                else
                {
                    Console.WriteLine("Błąd");
                }
            }
            return wynik.ToString();
        }
    }
}