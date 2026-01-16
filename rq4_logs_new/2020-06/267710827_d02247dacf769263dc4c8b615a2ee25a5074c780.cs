using System;
using System.Collections.Generic;
using System.Text;
using Data;

namespace Business
{
    public class ModeloPeticiones
    {
        PeticionesDao peticiones = new PeticionesDao();

        public List<string> nombresDirectivos()
        {
            return peticiones.nombresDirectivos();
        }
        public void add_user_request(int idUsuarioFrom, string nombre_directivo, string tipo_peticion, string asunto_solicitud, string descripcion_solicitud)
        {
            peticiones.add_user_request(idUsuarioFrom, nombre_directivo, tipo_peticion, asunto_solicitud, descripcion_solicitud);
        }

        public string mailDirect(string nombre_directivo)
        {
            return peticiones.mailDirect(nombre_directivo);
        }
    }
}