using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Clases
{
    internal class TouristRouts
    {
        public List<Sites> nodos; //Lists de sitios del grafo rutas turisticas

        //Constructor

        public TouristRouts()
        {
            nodos = new List<Sites>();

        }

        /// Operaciones basicas del grafo Rutas Turisticas ///
        

        //Construye un sitio a partir de sus parametros bases y lo agrega a la lista de sitios.
        public Sites AgregarSitio(string nombre, string descrip, float latitud, float longitud)
        {
            Sites nodo = new Sites(nombre, descrip, latitud,longitud);
            nodos.Add(nodo);
            return nodo;

        }

        //Agrega un sitio a la lista de nodos del grafo.
        public void AgregarSitio(Sites nuevoSitio)
        {
            nodos.Add(nuevoSitio);
        }

        //Busca un sitio en la lista de sitios del grafo.
        public Sites BuscarSitio(int id)
        {
            return nodos.Find(sitioID => sitioID.Id == id);
        }

        //Crear un Camino a partir de los valores de sitio de origen y sitio de destino
        public bool AgregarCamino(int o_IdSite, int f_IdSite, int peso)
        {
            Sites vOrigen, vnDestino;

            //Si alguno de los nodos no existen se activa la axcepcion.
            if ((vOrigen = nodos.Find(site => site.Id == o_IdSite)) == null)
                throw new Exception("El sition con id: " + o_IdSite + "No existe");
            if ((vnDestino = nodos.Find(site => site.Id == f_IdSite)) == null)
                throw new Exception("El sitio con id: "+f_IdSite+ "No existe" );

            return AgregarCamino(vOrigen, vnDestino, peso);

            
        }

        public bool AgregarCamino(Sites origen, Sites nDestino, int weight)
        {
            if (origen.ListaAdyacencia.Find(site => site.nsitio == nDestino) == null)
            {
                origen.ListaAdyacencia.Add(new Routes(nDestino, weight));
                return true;
            }

            return false;
        }

        public void DibujarTouristRoute()
        {

            
        }


    }
}