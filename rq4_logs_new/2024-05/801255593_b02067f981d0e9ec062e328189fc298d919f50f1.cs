using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClasesBase
{
    public class Usuarios
    {
        private readonly List<Usuario> usuarios;

        public Usuarios()
        {
            usuarios = new List<Usuario>()
            {
             new Usuario(1, "usu1", "usuario1", "Marisel Alarcón", 2),
             new Usuario(2, "usu2", "usuario2", "Maximiliano Pelazzo", 2),
             new Usuario(3, "usu3", "usuario3", "Maximiliano Martinez Gonzales", 2),
             new Usuario(4, "usu4", "usuario4", "Milton Delgado", 2),
             new Usuario(5, "admin", "admin", "Fernanda Sivila", 1)
            };
        }

        public List<Usuario> ObtenerUsuarios()
        {
            return usuarios;
        }
    }
}