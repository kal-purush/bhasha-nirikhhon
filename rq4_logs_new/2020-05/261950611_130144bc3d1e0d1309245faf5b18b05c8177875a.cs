using System;
using System.Collections.Generic;

namespace Table
{
    class myMatrix<TRow, TColumn, TValue>
    {
        /*Dictionary<TRow, Dictionary<TColumn, TValue>> row_dict = new Dictionary<TRow, Dictionary<TColumn, TValue>>();
        Dictionary<TColumn, TValue> col_dict = new Dictionary<TColumn, TValue>();
        public TValue this[TRow index, TColumn index2]
        {
            set
            {
                if (row_dict.ContainsKey(index) && col_dict.ContainsKey(index2))
                {
                    row_dict[index][index2] = value;
                }
                else if (row_dict.ContainsKey(index) && !col_dict.ContainsKey(index2))
                {
                    col_dict.Add(index2, value);
                    row_dict[index] = col_dict;
                }
                else if (!(row_dict.ContainsKey(index)) && col_dict.ContainsKey(index2))
                {
                    col_dict[index2] = value;
                    row_dict.Add(index, col_dict);
                }
                else
                {
                    Dictionary<TColumn, TValue> Tcol_dict = new Dictionary<TColumn, TValue>();
                    Tcol_dict[index2] = value;
                    row_dict.Add(index,Tcol_dict);
                }
            }
            get
            {
                return row_dict[index][index2];
            }
        }
        
        public void Print() {
            foreach (TRow row in row_dict.Keys) {
                foreach (TColumn col in row_dict[row].Keys) {
                    Console.WriteLine("{0},{1},{2}",row,col,row_dict[row][col]);
                }
            }
        }

        public void RemoveRow(TRow row) {
            row_dict.Remove(row);
        }

        public void RemoveColumn(TColumn col) {
            foreach (TRow row in row_dict.Keys) {
                row_dict[row].Remove(col);
            }
        }

        public myMatrix<TColumn, TRow, TValue> Transpose(){
            myMatrix<TColumn, TRow, TValue> result = new myMatrix<TColumn, TRow, TValue>();
            foreach (TRow row in row_dict.Keys)
            {
                foreach (TColumn col in row_dict[row].Keys)
                {
                    result[col,row] = row_dict[row][col];
                }
            }
            return result;
        }*/
        //map TRow to int
        Dictionary<TRow, int> row_map = new Dictionary<TRow, int>();
        //map TColumn to int
        Dictionary<TColumn, int> col_map = new Dictionary<TColumn, int>();
        int row_index = 0;
        int col_index = 0;
        //the 2-dimension array to save the Tvalue
        private TValue[,] arr = new TValue[100, 100];

        //indexer
        public TValue this[TRow row, TColumn col]
        {
            get
            {
                return arr[row_map[row], col_map[col]];
            }
            set
            {
                //if the value has been saved, update it
                if (row_map.ContainsKey(row) && col_map.ContainsKey(col))
                {
                    arr[row_map[row], col_map[col]] = value;
                }
                //update value for the specific situations
                else if (!row_map.ContainsKey(row) && col_map.ContainsKey(col))
                {
                    row_map[row] = row_index;
                    row_index++;
                    arr[row_map[row], col_map[col]] = value;
                }
                else if (row_map.ContainsKey(row) && !col_map.ContainsKey(col))
                {
                    col_map[col] = col_index;
                    col_index++;
                    arr[row_map[row], col_map[col]] = value;
                }
                //if the value isn't in the array
                else
                {
                    row_map[row] = row_index;
                    row_index++;
                    col_map[col] = col_index;
                    col_index++;
                    arr[row_map[row], col_map[col]] = value;
                }
            }
        }

        //print like a table
        public void Print()
        {
            foreach (TRow row in row_map.Keys)
            {
                foreach (TColumn col in col_map.Keys)
                {
                    Console.Write("{0}\t", arr[row_map[row], col_map[col]]);
                }
                Console.Write("\n");
            }
            Console.Write("\n");
        }

        public void RemoveRow(TRow row)
        {
            if (row_map.ContainsKey(row))
            {
                //remove the key in row_map
                //delete elements in array isn't easy
                row_map.Remove(row);
            }
            else
            {
                Console.WriteLine("The matrix doesn't contain this row");
            }
        }

        public void RemoveCol(TColumn col)
        {
            if (col_map.ContainsKey(col))
            {
                //remove the key in col_map
                col_map.Remove(col);
            }
            else
            {
                Console.WriteLine("The matrix doesn't contain this row");
            }
        }

        public myMatrix<TColumn, TRow, TValue> Transpose()
        {
            myMatrix<TColumn, TRow, TValue> trans = new myMatrix<TColumn, TRow, TValue>();
            foreach (TRow row in row_map.Keys)
            {
                foreach (TColumn col in col_map.Keys)
                {
                    //transpose the matrix O(n**2)
                    trans[col, row] = arr[row_map[row], col_map[col]];
                }
            }
            return trans;
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
            //Console.WriteLine( myIndexer["1", "1"]);
            myIndexer.Print();
            myMatrix<string, string, string> trans = myIndexer.Transpose();
            trans.Print();
            myIndexer.RemoveRow("1");
            myIndexer.Print();
            myIndexer.RemoveCol("1");
            myIndexer.Print();
        }
    }
}
