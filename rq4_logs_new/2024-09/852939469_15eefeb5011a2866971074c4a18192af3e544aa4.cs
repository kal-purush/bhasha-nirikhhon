// <copyright file="EntityConfiguration.cs" company="Andrey Nikolaev">
// Copyright (c) Andrey Nikolaev. All rights reserved.
// </copyright>

namespace DataAccess.Abstractions
{
    using Abstract.DataAccess;
    using Domain.Entities.Abstractions;
    using Microsoft.EntityFrameworkCore.Metadata.Builders;

    /// <summary>
    /// Конфигуратор сущности с названием.
    /// </summary>
    /// <typeparam name="TEntity"> Тип сущности. </typeparam>
    public abstract class EntityConfiguration<TEntity> : IdEntityConfiguration<TEntity>
        where TEntity : NamedEntity
    {
        /// <inheritdoc/>
        public override void Configure(EntityTypeBuilder<TEntity> builder)
        {
            base.Configure(builder);

            builder.Property(entity => entity.Name).IsRequired();
        }
    }
}