using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Lab7
{
    public class Department
    {
        int id_dep;
        String name_dep;

        public Department(int i, string s)
        {
            id_dep = i;
            name_dep = s;

        }

        public int ID_Dep
        {
            get
            {
                return id_dep;
            }

            set
            {
                id_dep = value;
            }
        }

        public String Name_Dep
        {
            get
            {
                return name_dep;
            }

            set
            {
                name_dep = value;
            }
        }

        public override string ToString()
        {
            return "(id=" + this.id_dep.ToString() + "; Отдел =" + this.name_dep + ")";
        }
    }
}