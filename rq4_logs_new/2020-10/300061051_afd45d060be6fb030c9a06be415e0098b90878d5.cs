using AutoFixture;
using EightCare.Domain.Common;
using System;
using System.Collections.Generic;

namespace EightCare.UnitTests.Extensions
{
    public static class FixtureExtensions
    {
        public static IEnumerable<TEntity> CreateManyWithIds<TEntity>(this IFixture fixture, params Guid[] ids)
            where TEntity : Entity
        {
            foreach (var id in ids)
            {
                var entity = fixture.Create<TEntity>();
                entity.SetId(id);

                yield return entity;
            }
        }
    }
}