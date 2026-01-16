using AccesoADatos;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PruebaAccesoADatos
{
    [TestClass]
    public class PruebaGestionEstadisticasExcepcion
    {
        [TestMethod]
        public void PruebaAgregarPuntajePorIdEstadisticaExcepcion()
        {
            int idEstadistica = -1;
            int puntajeAAgregar = 10;
            int resultadoEsperado = -1;
            int resultadoObtenido = GestionEstadisticas.AgregarPuntajePorIdEstadistica(idEstadistica, puntajeAAgregar);
            Assert.AreEqual(resultadoEsperado, resultadoObtenido);
        }

        [TestMethod]
        public void PruebaAgregarPartidaGanadaPorIdEstadisticaExcepcion()
        {
            int idEstadistica = -1;
            int resultadoEsperado = -1;
            int resultadoObtenido = GestionEstadisticas.AgregarPartidaGanadaPorIdEstadistica(idEstadistica);
            Assert.AreEqual(resultadoEsperado, resultadoObtenido);
        }

        [TestMethod]
        public void PruebaAgregarPartidaPerdidaPorIdEstadisticaExcepcion()
        {
            int idEstadistica = -1; 
            int resultadoEsperado = -1;
            int resultadoObtenido = GestionEstadisticas.AgregarPartidaPerdidaPorIdEstadistica(idEstadistica);
            Assert.AreEqual(resultadoEsperado, resultadoObtenido);
        }

        [TestMethod]
        public void PruebaObtenerEstadisticaPorIdEstadisticaExcepcion()
        {
            int idEstadistica = -1;
            Estadistica resultadoEsperado = null;
            Estadistica resultadoObtenido = GestionEstadisticas.ObtenerEstadisticaPorIdEstadistica(idEstadistica);
            Assert.AreEqual(resultadoEsperado, resultadoObtenido);
        }
    }
}