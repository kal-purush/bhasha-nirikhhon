// <copyright file="IdEntityConfiguration.cs" company="Andrey Nikolaev">
// Copyright (c) Andrey Nikolaev. All rights reserved.
// </copyright>

namespace Abstract.DataAccess
{
    using Domain.Entities.Abstractions;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Metadata.Builders;

    /// <summary>
    /// Конфигурация сущности с идентификатором.
    /// </summary>
    /// <typeparam name="TEntity"> Тип сущности. </typeparam>
    public abstract class IdEntityConfiguration<TEntity> : IEntityTypeConfiguration<TEntity>
        where TEntity : IdEntity
    {
        /// <inheritdoc/>
        public virtual void Configure(EntityTypeBuilder<TEntity> builder)
        {
            builder.Property(entity => entity.Id).IsRequired();
            builder.HasKey(entity => entity.Id);
        }
    }
}