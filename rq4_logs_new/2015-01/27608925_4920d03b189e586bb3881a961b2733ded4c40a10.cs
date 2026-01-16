using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace fileshare_utility
{
    interface IRepository
    {
        /// <summary>
        /// Retrieves an Entity from the Repository
        /// </summary>
        /// <typeparam name="T">Type of Entity</typeparam>
        /// <param name="Entity">Entity that is Like the entity to retrieve</param>
        /// <returns>Entity from Repository most alike the Entity provided</returns>
        T Get<T>(T Entity) where T : class, Entity<T>, new();

        /// <summary>
        /// Retrieves an Entity from the Repository, If none is found, the Entity provided is Inserted
        /// </summary>
        /// <typeparam name="T">Type of Entity</typeparam>
        /// <param name="Entity">Entity that is Like the entity to retrieve</param>
        /// <returns>Entity from Repository most alike the Entity provided</returns>
        T FindOrInsert<T>(T Entity) where T : class, Entity<T>, new();

        /// <summary>
        /// Inserts an Entity into the Repository
        /// </summary>
        /// <typeparam name="T">Type of Entity</typeparam>
        /// <param name="Entity">Entity to Insert</param>
        void Insert<T>(T Entity) where T : class, new();

        /// <summary>
        /// Initializes the output destination
        /// </summary>
        void InitOutput();
    }
}