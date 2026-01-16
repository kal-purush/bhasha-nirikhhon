// -----------------------------------------------------------------------
// <copyright file="DataColumn.cs" company="Carlos Huchim Ahumada">
// Este código se libera bajo los términos de licencia especificados.
// </copyright>
// -----------------------------------------------------------------------
namespace Jaguar.Reporting.Common
{
    using System;

    /// <summary>
    /// Información sobre la columna.
    /// </summary>
    public class DataColumn
    {
        /// <summary>
        /// Inicializa una nueva instancia de la clase <see cref="DataColumn"/>.
        /// </summary>
        /// <param name="columnName">Nombre de la columna.</param>
        public DataColumn(string columnName)
            : this(columnName, typeof(string))
        {
        }

        /// <summary>
        /// Inicializa una nueva instancia de la clase <see cref="DataColumn"/>.
        /// </summary>
        /// <param name="columnName">Nombre de la columna.</param>
        /// <param name="dataType">Tipo de datos.</param>
        public DataColumn(string columnName, Type dataType)
        {
            this.Name = columnName;
            this.Type = dataType;
        }

        /// <summary>
        /// Obtiene o establece el nombre de la columna.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Obtiene o establece el valor de la columna.
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// Obtiene o establece el tipo de datos.
        /// </summary>
        public Type Type { get; set; }
    }
}