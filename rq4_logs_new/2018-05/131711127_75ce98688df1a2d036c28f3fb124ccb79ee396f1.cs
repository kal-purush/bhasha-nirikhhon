using DockService.Core.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DockService.Core.Repositories
{
    public interface IShipRepository
    {
        /// <summary>
        /// Creates the ship.
        /// </summary>
        /// <param name="ship">The ship.</param>
        /// <returns></returns>
        Task<Ship> CreateShip(Ship ship);

        /// <summary>
        /// Deletes the ship.
        /// </summary>
        /// <param name="Id">The identifier.</param>
        /// <returns></returns>
        Task DeleteShip(Guid Id);

        /// <summary>
		/// Gets the ship.
		/// </summary>
		/// <param name="Id">The identifier.</param>
		/// <returns></returns>
		Task<Ship> GetShip(Guid Id);

        /// <summary>
		/// Updates ship.
		/// </summary>
		/// <param name="ship">The ship to be updated</param>
		/// <returns></returns>
		Task UpdateShip(Ship ship);
    }
}