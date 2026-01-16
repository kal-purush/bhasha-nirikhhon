// <copyright file="CrudService.cs" company="Andrey Nikolaev">
// Copyright (c) Andrey Nikolaev. All rights reserved.
// </copyright>

namespace Services.Abstractions
{
    using Domain.Entities.Abstractions;
    using Microsoft.EntityFrameworkCore;
    using Services.Abstractions.Interfaces;

    /// <summary>
    /// Сервис по работе с CRUD методами.
    /// </summary>
    /// <typeparam name="TEntity"> Тип целевой сущности. </typeparam>
    /// <typeparam name="TDataContext"> Контекст базы данных. </typeparam>
    public abstract class CrudService<TEntity, TDataContext> : ReadOnlyService<TEntity, TDataContext>, ICrudService<TEntity>
        where TEntity : class, IIdEntity
        where TDataContext : DbContext
    {
        /// <summary>
        /// Инициализирует новый экземпляр класса <see cref="CrudService{TEntity, TDataContext}"/>.
        /// </summary>
        /// <param name="dataContext"> Контекст базы данных. </param>
        public CrudService(TDataContext dataContext)
            : base(dataContext)
        {
        }

        /// <inheritdoc/>
        public async Task<TEntity> CreateAsync(TEntity entity, bool withId = false)
        {
            var entry = this.DataContext.Attach(entity);

            if (!withId)
            {
                entry.State = EntityState.Added;
            }

            await this.DataContext.SaveChangesAsync();

            return entry.Entity;
        }

        /// <inheritdoc/>
        public bool Delete(TEntity entity)
        {
            _ = this.DataContext.Remove(entity);
            return true;
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteAsyns(Guid id)
        {
            var entity = await this.GetIdAsync(id);

            if (entity is null)
            {
                return false;
            }

            return this.Delete(entity);
        }

        /// <inheritdoc/>
        public async Task<TEntity> UpdateAsync(TEntity entity)
        {
            var entry = this.DataContext.Attach(entity);

            entry.State = EntityState.Modified;

            await this.DataContext.SaveChangesAsync();

            return entry.Entity;
        }
    }
}