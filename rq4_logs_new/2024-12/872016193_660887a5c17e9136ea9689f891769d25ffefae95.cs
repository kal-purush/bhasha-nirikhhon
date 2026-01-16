using log4net;
using System;
using System.Collections.Generic;
using System.Data.Entity.Core;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AccesoADatos
{
    public class GestionLogros
    {
        private static readonly ILog _bitacora = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public int RegistrarLogroPorIdJugador(int idJugador, int idLogro) 
        {
            int resultadoRegitroLogro = 0;
            try
            {
                using (var contexto = new PasswordEntidades())
                {
                    var detalleLogro = new DetalleLogro() 
                    {
                        FKIdJugador = idJugador,
                        FKIdLogro = idLogro,
                    };
                    contexto.DetalleLogro.Add(detalleLogro);
                    resultadoRegitroLogro = contexto.SaveChanges();
                }
            }
            catch (DbUpdateException excepcionActualizacion)
            {
                _bitacora.Warn(excepcionActualizacion);
                resultadoRegitroLogro = -1;
            }
            catch (EntityException excepcionEntidad)
            {
                _bitacora.Error(excepcionEntidad);
                resultadoRegitroLogro = -1;
            }
            return resultadoRegitroLogro;
        }

        public static int VerificarRegistroLogroPorIdJugador(int idJugador, int idLogro) 
        {
            int verificacionLogro = 0;
            try
            {
                using (var contexto = new PasswordEntidades())
                {
                    bool logroExistente = contexto.DetalleLogro
                    .Any(detalleLogro => detalleLogro.FKIdJugador == idJugador && detalleLogro.FKIdLogro == idLogro);                    
                    if (logroExistente)
                    {
                        verificacionLogro = 1;
                    }
                }
            }
            catch (EntityException excepcionEntidad)
            {
                _bitacora.Error(excepcionEntidad);
                verificacionLogro = -1;
            }
            return verificacionLogro;
        }

        public static List<int> ObtenerLogrosPorIdJugador(int idJugador) 
        {
            List<int> logros = new List<int>();
            try
            {
                using (var contexto = new PasswordEntidades())
                {                    
                    logros = contexto.DetalleLogro
                        .Where(detalleLogro => detalleLogro.FKIdJugador == idJugador)
                        .Select(detalleLogro => detalleLogro.FKIdLogro)
                        .ToList();
                }
            }
            catch (EntityException excepcionEntidad)
            {
                _bitacora.Error(excepcionEntidad);
                logros.Insert(0, -1);
            }
            return logros;
        }

        public static int VerificarCatalogoLogros()
        {
            int resultadoVerificacion = 0;
            try
            {
                using (var contexto = new PasswordEntidades())
                {                    
                    int totalLogros = contexto.Logro.Count();
                    if (totalLogros >= 5)
                    {
                        resultadoVerificacion = 1;
                    }                    
                }
            }
            catch (EntityException excepcionEntidad)
            {
                _bitacora.Error(excepcionEntidad);
                resultadoVerificacion = -1; 
            }
            return resultadoVerificacion;
        }

        public static int VerificarCumplimientoPrimerLogroPorIdEstadistica(int idEstadistica) 
        {
            int resultadoVerificacion = 0;
            try
            {
                using (var contexto = new PasswordEntidades())
                {
                    var estadistica = contexto.Estadistica.FirstOrDefault(entidad => entidad.idEstadistica == idEstadistica);                    
                    if (estadistica != null && estadistica.partidasGanadas > 0)
                    {
                        resultadoVerificacion = 1;
                    }
                }
            }
            catch (EntityException excepcionEntidad)
            {
                _bitacora.Error(excepcionEntidad);
                resultadoVerificacion = -1;
            }
            return resultadoVerificacion;
        }

        public static int VerificarCumplimientoSegundoLogroPorIdEstadistica(int idEstadistica)
        {
            int resultadoVerificacion = 0;
            try
            {
                using (var contexto = new PasswordEntidades())
                {
                    var estadistica = contexto.Estadistica.FirstOrDefault(entidad => entidad.idEstadistica == idEstadistica);
                    if (estadistica != null && estadistica.partidasGanadas >= 10)
                    {
                        resultadoVerificacion = 1;
                    }
                }
            }
            catch (EntityException excepcionEntidad)
            {
                _bitacora.Error(excepcionEntidad);
                resultadoVerificacion = -1;
            }
            return resultadoVerificacion;
        }       
    }
}