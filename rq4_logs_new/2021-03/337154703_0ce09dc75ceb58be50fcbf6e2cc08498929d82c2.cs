using System;
using System.Collections.Generic;
using TennisClub.AppCore.model.impl;

namespace TennisClub.Infrastructure.interfaces
{
    public interface IGroupFacade
    {
        public Group FindGroup(Guid id);
        public List<Group> GetAll();
    }
}