using Clases;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace chevere_master
{
     class CLista
    {
        private CVertice aElemento;
        private CLista aSubLista;
        private int aPeso;

        //contructores 
        public CLista()
        {
            aElemento = null;
            aSubLista = null;
            aPeso = 0;
        }

        public CLista(CLista pLista)
        {
            if (pLista != null)
            {
                aElemento = pLista.aElemento;
                aSubLista = pLista.aSubLista;
                aPeso = pLista.aPeso;
            }
        }
        public CLista(CVertice pElement, CLista pSubLista, int pPeso)
        {
            aElemento = pElement;
            aSubLista = pSubLista;
            aPeso = pPeso;
        }
        //propiedades
        public CVertice Elemento
        {
            get { return aElemento; }
            set { aElemento = value; }
        }
        public CLista SubLista
        {
            get { return aSubLista; }
            set { aSubLista = value; }
        }

        public int Peso
        {
            get { return aPeso; }
            set { aPeso = value; }
        }
        //métodos
        public bool EsVacia()//verificar si la lista está vacía
        {
            return aElemento == null;
        }

        public void Agregar(CVertice pElemento, int pPeso)
        {
            if (pElemento != null)
            {
                if (aElemento == null)
                {
                    aElemento = new CVertice(pElemento.Valor); //posiblre error
                    aPeso = pPeso;
                    aSubLista = new CLista();
                }
                else
                {
                    if (!ExisteElemento(pElemento))
                        aSubLista.Agregar(pElemento, pPeso);
                }
            }
        }
        public void Eliminar(CVertice pElemento)
        {
            if (aElemento != null)
            {
                if (aElemento.Equals(pElemento))
                {
                    aElemento = aSubLista.aElemento;
                    aSubLista = aSubLista.aSubLista;
                }
            }
            else aSubLista.Eliminar(pElemento);
         
        }
        public int NroElementos()
        {
            if (aElemento != null)
                return 1 + aSubLista.NroElementos();
            else return 0;
        }
        public object lesimoElemento(int posicion)
        {
            if ((posicion > 0) && (posicion <= NroElementos()))
            {
                if (posicion == 1)
                {
                    return aElemento;
                }
                else
                {
                    return aSubLista.lesimoElemento(posicion - 1);
                }
            }
            else return null;
        }

        public object lesimoElementoPeso(int posicion)
        {
            if ((posicion > 0) && (posicion <= NroElementos()))
            {
                if (posicion == 1)
                {
                    return aPeso;
                }
                else
                {
                    return aSubLista.lesimoElemento(posicion - 1);
                }
            }
            else return 0;
        }

        public bool ExisteElemento(CVertice pElemento)
        {
            if (aElemento != null && (pElemento != null))
            {
                return (
                    aElemento.Equals(pElemento) ||
                    aSubLista.ExisteElemento(pElemento)
                    );
            }
            else return false;
        }
        public int PosicionElemento(CVertice pElemento)
        {
            if ((aElemento != null) || (ExisteElemento(pElemento)))
            {
                if ((aElemento.Equals(pElemento))) return 1;
                else return 1 + aSubLista.PosicionElemento(pElemento);
            }
            else return 0;
        }

    }
}