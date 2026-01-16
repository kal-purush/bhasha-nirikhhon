using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Andamio.Data.Entities;

namespace Andamio.Data.Access
{
    /// <typeparam name="EntityType"></typeparam>
    public interface IDao<EntityType>
        where EntityType : EntityBase, new()
    {
        #region Update\Insert
        /// <summary>
        /// Updates or Insert the specified Entity in the Data Source.
        /// </summary>
        /// <param name="entity">The Entity to Insert or Update.</param>
        void Upsert(EntityType entity);

        /// <summary>
        /// Inserts the specified Entity in the Data Source.
        /// </summary>
        /// <param name="entity">The Entity to Insert.</param>
        void Insert(EntityType entity);

        /// <summary>
        /// Updates the specified Entity in the Data Source.
        /// </summary>
        /// <param name="entity">The Entity to Update.</param>
        void Update(EntityType entity);

        #endregion

        #region Delete
        /// <summary>
        /// Deletes the specified Entity from the Data Source.
        /// </summary>
        /// <param name="entity">The Entity to delete from the Data Source.</param>
        void Delete(EntityType entity);

        #endregion

        #region Retrieval
        /// <summary>
        /// Retrieves all Entities in the Data Source.
        /// </summary>
        List<EntityType> All();

        /// <summary>
        /// Retrieves all Entities from the Data Source for the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate function to filter the Entities in the Data Source.</param>
        List<EntityType> Many(Expression<Func<EntityType, bool>> predicate);

        /// <summary>
        /// Retrieves a single Entity from the Data Source for the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate function to filter the Entities in the Data Source.</param>
        EntityType Single(Expression<Func<EntityType, bool>> predicate);

        /// <summary>
        /// Retrieves an Entity from the Data Source for the specified Primary Key.
        /// </summary>
        /// <param name="primaryKey">The Primary Key that indentifies the Entity.</param>
        EntityType WithKey<EntityKey>(EntityKey primaryKey)
            where EntityKey : struct, IComparable<EntityKey>;

        #endregion

        #region Search
        /// <summary>
        /// Retrieves all Entities from the Data Source for the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate function to filter the Entities in the Data Source.</param>
        List<EntityType> Search<SortKey>(Expression<Func<EntityType, bool>> predicate
            , Expression<Func<EntityType, SortKey>> sort
            , SearchPageSettings pageSettings = null);

        /// <summary>
        /// Retrieves all Entities from the Data Source for the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate function to filter the Entities in the Data Source.</param>
        List<EntityType> Search<SortKey>(DaoQuery<EntityType> query
            , Expression<Func<EntityType, SortKey>> sort
            , SearchPageSettings pageSettings = null);

        /// <summary>
        /// Retrieves all Entities from the Data Source for the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate function to filter the Entities in the Data Source.</param>
        DaoQuery<EntityType> Query(Expression<Func<EntityType, bool>> predicate);

        /// <summary>
        /// Retrieves all Entities from the Data Source for the specified predicate.
        /// </summary>
        DaoQuery<EntityType> Query();

        #endregion

        #region Entity
        DaoEntity<EntityType> Entity(EntityType entity);

        #endregion
    }
}