/*
Copyright © 2017-2019 César Andrés Morgan
Licenciado para uso interno solamente.
*/

using System.Collections.Generic;
using System;

namespace TheXDS.Proteus.Component
{
    public interface IModelLocalSearchFilter
    {
        TCollection Filter<TCollection>(TCollection collection, string query) where TCollection : IList<Models.Base.ModelBase>, new();

        /// <summary>
        /// Comprueba si este <see cref="IModelSearchFilter"/> puede crear
        /// filtros para el modelo especificado.
        /// </summary>
        /// <param name="model">Modelo a comprobar.</param>
        /// <returns>
        /// <see langword="true"/> si esta instancia puede crear filtros de
        /// consulta para el modelo especificado, <see langword="false"/>
        /// en caso contrario.
        /// </returns>
        bool UsableFor(Type model);
    }
}