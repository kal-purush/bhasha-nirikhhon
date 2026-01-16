using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Runtime;

namespace WindowsFormsApp8
{
    public partial class Form1 : Form
    {
        // доп.функция генерации перестановок маршрута
        public int[] swap(int[] ar, int i, int j)
        {
            int a = ar[i];
            ar[i] = ar[j];
            ar[j] = a;
            return ar;
        }
        // доп.функция генерации маршрута
        public bool permNxt(int[] ar,  int a)
        {
            int rt = ar.Length - 1; 
                int i = rt - 1;
            int j = rt;
            while ((i >= a) && (ar[i] >= ar[i + 1]))
                i--;      
            if (i < a)                               
                return false;       
                j = rt;                                    
                while (ar[i] >= ar[j]) j--;               
                ar = swap(ar, i, j);
                a = i + 1;                                  
                while (a < rt)
                ar = swap(ar, a++, rt--);
            return true;                                 
            
        }
        // доп.функция вычисления длины пути по маршруту
        public float dist_culc (float[,] dis_matrix, int[] order)
        {
            float dist = 0;
            int i;

            for (i=0; i<= order.Length; i++)
            {
                if (i == 0)  
                {
                    dist = dist + dis_matrix[0,order[i]];
                }
                else if ((i > 0) && (i< (order.Length)))
                {
                    dist = dist + dis_matrix[order[i], order[i-1]];
                }
                else if  (i == (order.Length))
                {
                dist = dist + dis_matrix[order[i - 1], 0];
                }

        }
            return dist;
        }
        // Сама функция перебора
        public void brute_forcse_komi (float[,] dis_matrix)
        {
            int[] city_array = new int[dis_matrix.GetLength(0)-1];
            int[] answer_array = new int[dis_matrix.GetLength(0)-1];
            float minimal_dist = 0;
            float new_dist = 0;
            for (int i = 0; i < (dis_matrix.GetLength(0)-1); i++)
                city_array[i] = (i+1);
                answer_array = city_array;
            minimal_dist = dist_culc(dis_matrix, city_array);

            while (permNxt(city_array, 0))
            {
                new_dist = dist_culc(dis_matrix, city_array);
                if (minimal_dist> new_dist)
                {
                    minimal_dist = new_dist;
                    answer_array = city_array;
                }
            }

            // Вывод пути и длины пути в текстбокс
           
            richTextBox1.Text =  "0";
            for (int j =0; j< answer_array.Length; j++)
                richTextBox1.Text = richTextBox1.Text + answer_array[j].ToString();
            richTextBox1.Text = richTextBox1.Text +  '0';
            richTextBox1.Text = richTextBox1.Text + '\n' + minimal_dist.ToString();

        }

        public Form1()
        {
            InitializeComponent();
            
        }
        
        private void button1_Click(object sender, EventArgs e)
        {
            // инициализация матрицы смежности для анализа
            float[,] smij_matrix = new float[5, 5] { {0,81,88,47,84},{81,0,28,53,48},{88,28,0,46,23},{47,53,46,0,37 },{84,48,23,37,0} };
            // вызов функции
            brute_forcse_komi(smij_matrix);
        }

    }
}