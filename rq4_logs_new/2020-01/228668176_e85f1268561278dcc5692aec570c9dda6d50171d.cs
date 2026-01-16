/*
Copyright © 2017-2019 César Andrés Morgan
Licenciado para uso interno solamente.
*/

using System;

namespace TheXDS.Proteus.Plugins
{
    /// <summary>
    /// Describe diferentes modos de visibilidad para un
    /// <see cref="CrudTool"/>.
    /// </summary>
    [Flags]
    public enum CrudToolVisibility : byte
    {
        /// <summary>
        /// Mostrar para la vista de Crud sin selección.
        /// </summary>
        Unselected = 1,

        /// <summary>
        /// Mostrar para la vista de Crud con una entidad seleccionada.
        /// </summary>
        Selected,

        /// <summary>
        /// Mostrar para la vista de Crud con o sin selección.
        /// </summary>
        NotEditing,

        /// <summary>
        /// Mostrar para la vista de Crud en modo de edición.
        /// </summary>
        Editing,

        /// <summary>
        /// Mostrar para las vistas de Crud sin selección y modo de edición.
        /// </summary>
        EditAndUnselected,

        /// <summary>
        /// Mostrar para las vistas de Crud con una entidad seleccionada y modo
        /// de edición.
        /// </summary>
        EditAndSelected,

        /// <summary>
        /// Mostrar en todas las vistas de Crud.
        /// </summary>
        Everywhere
    }
}