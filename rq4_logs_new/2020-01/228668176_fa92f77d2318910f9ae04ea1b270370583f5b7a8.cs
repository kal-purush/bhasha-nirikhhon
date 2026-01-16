/*
Copyright © 2017-2019 César Andrés Morgan
Licenciado para uso interno solamente.
*/

namespace TheXDS.Proteus.Config
{
    /// <summary>
    /// Describe distintos modos de UI de Reporte de operación.
    /// </summary>
    public enum UiMode : byte
    {
        /// <summary>
        /// Modo simple.
        /// </summary>
        Simple,
        /// <summary>
        /// Modo plano.
        /// </summary>
        Flat,
        /// <summary>
        /// Modo mínimo.
        /// </summary>
        Minimal,
        /// <summary>
        /// Con Logging
        /// </summary>
        Logging
    }
}