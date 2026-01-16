using System;
using System.Collections.Generic;
using System.Linq;

namespace Table
{
    class myMatrix<TRow, TColumn, TValue>
    {
        //map TRow to int
        private Dictionary<TRow, int> row_map = new Dictionary<TRow, int>();
        //map TColumn to int
        private Dictionary<TColumn, int> col_map = new Dictionary<TColumn, int>();
        int row_index = 0;
        int col_index = 0;
        private List<List<TValue>> arr = new List<List<TValue>>();
        //the nesting list to save the Tvalue
        private Dictionary<int, List<TValue>> nest_dict = new Dictionary<int, List<TValue>>();
        public TValue this[TRow row, TColumn col]
        {
            set
            {
                if (row_map.ContainsKey(row) && col_map.ContainsKey(col))
                {
                    //if the value has been saved, update it
                    try
                    {
                        nest_dict[row_map[row]][col_map[col]] = value;
                        arr[row_map[row]][col_map[col]] = value;
                    }
                    //if not, save the value(for example, (1,1)and(1,2)has been saved, 
                    //(2,1)will go into this situation but you can't update its value directly)
                    catch (ArgumentOutOfRangeException)
                    {
                        nest_dict[row_map[row]].Add(value);
                        arr[row_map[row]] = nest_dict[row_map[row]];
                        //Console.WriteLine(arr[row_map[row]].Count);
                    }
                }
                //update value for the specific situations
                else if (!row_map.ContainsKey(row) && col_map.ContainsKey(col))
                {
                    row_map[row] = row_index;
                    row_index++;
                    nest_dict[row_map[row]] = new List<TValue>();
                    nest_dict[row_map[row]].Add(value);
                    arr.Add(nest_dict[row_map[row]]);
                }
                else if (row_map.ContainsKey(row) && !col_map.ContainsKey(col))
                {
                    col_map[col] = col_index;
                    col_index++;
                    nest_dict[row_map[row]].Add(value);
                    arr[row_map[row]] = nest_dict[row_map[row]];
                    //Console.WriteLine(arr[0][0]);
                }
                //if the value isn't in the array
                else
                {
                    row_map[row] = row_index;
                    row_index++;
                    col_map[col] = col_index;
                    col_index++;
                    nest_dict[row_map[row]] = new List<TValue>();
                    nest_dict[row_map[row]].Add(value);
                    arr.Add(nest_dict[row_map[row]]);
                    //Console.WriteLine(arr[0][0]);
                }
            }
            get
            {
                return arr[row_map[row]][col_map[col]];
            }
        }

        public void Print()
        {
            //print in a table format
            //Console.WriteLine(arr[0][0]);
            foreach (TRow row in row_map.Keys)
            {
                foreach (TColumn col in col_map.Keys)
                {
                    try
                    {
                        //if this position has value, show it
                        Console.Write("{0}\t", arr[row_map[row]][col_map[col]]);
                    }
                    catch (ArgumentOutOfRangeException)
                    {
                        //otherwise print null
                        Console.Write("null\t");
                    }
                }
                Console.Write("\n");
            }
            Console.Write("\n");
        }

        public void RemoveRow(TRow row)
        {
            int temp = row_map[row];
            arr.RemoveAt(row_map[row]);
            //update the mapping dictionary
            foreach (TRow key in row_map.Keys.ToList())
            {
                if (row_map[key] == temp)
                {
                    row_map.Remove(key);
                }
                else if (row_map[key] > temp)
                {
                    row_map[key] -= 1;
                }
                else
                {
                    break;
                }
            }
        }

        public void RemoveCol(TColumn col)
        {
            int temp = col_map[col];
            //update array
            foreach (TRow row in row_map.Keys)
            {
                if (col_map.ContainsKey(col))
                {
                    arr[row_map[row]].RemoveAt(temp);
                }
            }
            //update col_dict(the column mapping dictionary)
            foreach (TColumn key in col_map.Keys.ToList())
            {
                if (col_map[key] == temp)
                {
                    col_map.Remove(key);
                }
                else if (col_map[key] > temp)
                {
                    col_map[key] -= 1;
                }
                else
                {
                    break;
                }
            }
        }

        //transpose matrix
        public myMatrix<TColumn, TRow, TValue> Transpose()
        {
            myMatrix<TColumn, TRow, TValue> result = new myMatrix<TColumn, TRow, TValue>();
            foreach (TRow row in row_map.Keys)
            {
                foreach (TColumn col in col_map.Keys)
                {
                    try
                    {
                        result[col, row] = arr[row_map[row]][col_map[col]];
                    }
                    catch (ArgumentOutOfRangeException)
                    {
                    }
                }
            }
            return result;
        }
    }
    class Program
    {

        static void Main(string[] args)
        {
            //testing
            myMatrix<string, string, string> myIndexer = new myMatrix<string, string, string>();
            myIndexer["1", "1"] = "22";
            myIndexer["1", "2"] = "23";
            myIndexer["2", "1"] = "44";
            myIndexer["2", "2"] = "46";
            myIndexer["3", "2"] = "55";
            myIndexer.Print();
            //Console.WriteLine(myIndexer["1", "2"]);
            myMatrix<string, string, string> trans = myIndexer.Transpose();
            trans.Print();
            myIndexer.RemoveRow("1");
            myIndexer.Print();
            myIndexer.RemoveCol("1");
            myIndexer.Print();
        }
    }
}
