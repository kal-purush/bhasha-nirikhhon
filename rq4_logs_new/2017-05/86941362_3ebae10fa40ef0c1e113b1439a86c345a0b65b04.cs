using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Artigo
{
    public class DataLogin
    {
        string dia;
        string mes;
        string ano;

        string data;
        public string retornarData()
        {
             dia = DateTime.Now.Day.ToString();
             mes = DateTime.Now.Month.ToString();
             ano = DateTime.Now.Year.ToString();
            //passa a data para o Label e apresenta na tela

            return data = "0" + dia + "/" + "0" + mes + "/" + ano;
        }
        //recebe a data atual

    }
}