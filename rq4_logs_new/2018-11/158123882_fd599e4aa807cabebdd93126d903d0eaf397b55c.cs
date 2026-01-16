using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WindowsFormsApp5
{
    class ClassQueue
    {
        Queue<int> qu;
        public int count;

        public ClassQueue()
        {
            qu = new Queue<int>();
            
        }

        public void CreateArray(string s)
        {
            FormArray formedArray = new FormArray(s);           //что это? поможет ли это
            int[] arrayOfNums = formedArray.arrayOfNums;    // как мне обратиться к полю объекта класса Class2?
            
            for (int i = 0; i < arrayOfNums.Length; i++)
            {
                count++;
            }


        }
    }


}