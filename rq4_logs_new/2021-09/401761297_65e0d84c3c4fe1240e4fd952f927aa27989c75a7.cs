using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Disney.Application.Authentication
{
    public class AuthValidationResponses 
    {
        public const string UserAlreadyExist = "Ya existe este nombre de usuario!";
        public const string UserOrPasswordAreIncorrect = "Whooops, parece que la contraseña o el usuario son incorrectos!";
        public const string UserDoesNotExist = "No se ha podido encontrar el usuario";
    }
}