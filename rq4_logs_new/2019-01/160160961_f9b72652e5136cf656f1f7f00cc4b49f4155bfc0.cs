using System;

//Classe NullCibleException

namespace Cast.Package.Exceptions
{
    //NullCibleException hérite de la classe NullReferenceException
    public class NullCibleException : NullReferenceException
    {
        //Constructeur de NullCibleException, on appel le constructeur de la classe mère avec en paramètre une string personnalisée
        public NullCibleException() : base("NullCibleException : La cible n'est pas définis.") { }
    }
}