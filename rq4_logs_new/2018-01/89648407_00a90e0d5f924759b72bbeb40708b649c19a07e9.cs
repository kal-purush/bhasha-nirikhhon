using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CE.Model
{
    public class ParametrosDeArchivo
    {
        private string tipo;
        private string funcionSql;
        private string archivo;
        private string nameSpace;
        private string esquema;

        public string Tipo
        {
            get
            {
                return tipo;
            }

            set
            {
                tipo = value;
            }
        }

        public string FuncionSql
        {
            get
            {
                return funcionSql;
            }

            set
            {
                funcionSql = value;
            }
        }

        public string Archivo
        {
            get
            {
                return archivo;
            }

            set
            {
                archivo = value;
            }
        }

        public string NameSpace
        {
            get
            {
                return nameSpace;
            }

            set
            {
                nameSpace = value;
            }
        }

        public string Esquema
        {
            get
            {
                return esquema;
            }

            set
            {
                esquema = value;
            }
        }
    }
}