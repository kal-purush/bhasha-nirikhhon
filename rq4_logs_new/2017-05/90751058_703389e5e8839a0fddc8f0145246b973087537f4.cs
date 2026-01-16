// -----------------------------------------------------------------------
// <copyright file="NoDataException.cs" company="Carlos Huchim Ahumada">
// Este código se libera bajo los términos de licencia especificados.
// </copyright>
// -----------------------------------------------------------------------
namespace Jaguar.Reporting
{
    using System;

    /// <summary>
    /// Excepción ocurrida cuando no existen registros en una tabla.
    /// </summary>
    public class NoDataException : Exception
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase <see cref="NoDataException"/>.
        /// </summary>
        public NoDataException() : base("No existen registros en la tabla.")
        {
        }

        /// <summary>
        /// Inicializa una nueva instancia de la clase <see cref="NoDataException"/>.
        /// </summary>
        /// <param name="message"></param>
        public NoDataException(string message) : base(message)
        {
        }

        /// <summary>
        /// Inicializa una nueva instancia de la clase <see cref="NoDataException"/>.
        /// </summary>
        /// <param name="table">Nombre de la tabla.</param>
        /// <param name="message"></param>
        public NoDataException(string table, string message) : base($"No exisen registros en la tabla {table}. {message}")
        {
        }

        /// <summary>
        /// Inicializa una nueva instancia de la clase <see cref="NoDataException"/>.
        /// </summary>        
        /// <param name="message">Mensaje de error.</param>
        /// <param name="ex">Error interno.</param>
        public NoDataException(string message, Exception ex) : base(message, ex)
        {
        }
    }
}