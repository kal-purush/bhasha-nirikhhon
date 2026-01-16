using System;
using System.Linq;

namespace Arbol1._2.consola
{
    class ManejadorArbol
    {
        //METODO RECURSIVO, PORQUE SE LLAMA A SI MISMO  
        public string ImprimirArbolInfijo(Nodo nodo)
        {
            //analiza el conenido de una hoja 
            if (!nodo.Hijos.Any())
                return nodo.Valor;

            //analizo cuando no soy hoja 
            return $"( {ImprimirArbolInfijo(nodo.Hijos[0])}  {nodo.Valor} {ImprimirArbolInfijo(nodo.Hijos[1])} )";
        }

        internal string ImprimirArbolPrefijo(Nodo nodo)
        {
            //analiza el conenido de una hoja 
            if (!nodo.Hijos.Any())
                return nodo.Valor;
            //analizo cuando no soy hoja
            return $"( {nodo.Valor} {ImprimirArbolPrefijo(nodo.Hijos[0])}  {ImprimirArbolPrefijo(nodo.Hijos[1])} )";
        }

        internal string ImprimirArbolPosfijo(Nodo nodo)
        {
            //analiza el conenido de una hoja 
            if (!nodo.Hijos.Any())
                return nodo.Valor;

            return $"( {ImprimirArbolPosfijo(nodo.Hijos[0])} {ImprimirArbolPosfijo(nodo.Hijos[1])}  {nodo.Valor} ) ";
        }

        internal string ImprimirArbol(Nodo nodo, Notacion notacion)
        {
            //analiza el conenido de una hoja 
            if (!nodo.Hijos.Any())
                return nodo.Valor;
            switch (notacion)
            {
                case Notacion.Infijo:
                    return $"( {ImprimirArbol(nodo.Hijos[0], notacion)}  {nodo.Valor} {ImprimirArbol(nodo.Hijos[1], notacion)} )";
                case Notacion.Prefijo:
                    return $"( {nodo.Valor} {ImprimirArbol(nodo.Hijos[0], notacion)}  {ImprimirArbol(nodo.Hijos[1], notacion)} )";
                case Notacion.Posfijo:
                    return $"( {ImprimirArbol(nodo.Hijos[0], notacion)} {ImprimirArbol(nodo.Hijos[1], notacion)}  {nodo.Valor} ) ";
                default:
                    return "tipo de notacion no identificada";
            }
            
            return notacion switch
            {
                Notacion.Infijo => $"( {ImprimirArbol(nodo.Hijos[0], notacion)}  {nodo.Valor} {ImprimirArbol(nodo.Hijos[1], notacion)} )",
                Notacion.Prefijo => $"( {nodo.Valor} {ImprimirArbol(nodo.Hijos[0], notacion)}  {ImprimirArbol(nodo.Hijos[1], notacion)} )",
                Notacion.Posfijo => $"( {ImprimirArbol(nodo.Hijos[0], notacion)} {ImprimirArbol(nodo.Hijos[1], notacion)}  {nodo.Valor} ) ",
                _ => "tipo de notacion no identificada",
            };
        }
    }
}