using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Automobilka.Responsivity
{
    class ClassTest
    {
        private int top;
        private BackgroundWorker worker;
        public ClassTest(BackgroundWorker worker)
        {
            this.worker = worker;
            top = 1000;
        }

        public int calculate()
        {
            int sum = 0;
            int iterator = 1;
            while(iterator < top)
            {
                sum += iterator;
                worker.ReportProgress(iterator/10);
                iterator++;
            }
            return sum;
        }
    }
}