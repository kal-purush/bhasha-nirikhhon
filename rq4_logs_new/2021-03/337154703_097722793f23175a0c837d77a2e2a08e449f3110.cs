using System.Collections.Generic;
using System.Linq;
using TennisClub.AppCore.model.impl;
using TennisClub.Data.model;

namespace TennisClub.Infrastructure.mappers
{
    public class ChildInDbNullableToChildMapper : IMapper<ChildInDb, Child>
    {
        public Child Map(ChildInDb entity)
        {
            if (entity != null)
                return new Child(
                    firstName: entity.FirstName,
                    lastName: entity.LastName,
                    gameLevel: entity.GameLevel,
                    preferableDay: entity.PreferableDay,
                    birthday: entity.Birthday,
                    id: entity.Id);
            else return null;
        }
    }
}