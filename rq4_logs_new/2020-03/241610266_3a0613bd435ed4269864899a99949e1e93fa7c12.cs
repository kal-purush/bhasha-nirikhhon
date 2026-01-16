using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Workers.Classes
{
    public abstract class BaseWorker : IComparable
    {
        public string Name { get; set; }
        public DateTime BirthDate { get; set; }

        protected BaseWorker(string name, DateTime birthDate)
        {
            Name = name;
            BirthDate = birthDate;
        }

        public abstract double CalcSalary();

        public int CompareTo(object obj)
        {
            if (obj is BaseWorker)
            {
                if (CalcSalary() == ((BaseWorker)obj).CalcSalary())
                    return 0;
                else if (CalcSalary() > ((BaseWorker)obj).CalcSalary())
                    return +1;
                else
                    return -1;
            }
            else if (obj is null)
                throw new ArgumentNullException("Попытка сравнения с пустотой");
            else
                throw new ArgumentException("Попытка сравнения с " + obj.GetType().Name, nameof(obj));
        }

    }
}