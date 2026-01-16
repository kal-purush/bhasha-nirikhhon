using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using TennisClub.AppCore.interfaces;
using TennisClub.AppCore.model.impl;
using TennisClub.AppCore.model.interfaces;
using TennisClub.AppCore.validators;
using TennisClub.Data.dao;
using TennisClub.Infrastructure.interfaces;
using TennisClub.Infrastructure.mappers;
using TennisClub.Infrastructure.services;

namespace TennisClub.Infrastructure.pipelines
{
    public class ChildFacade : IChildFacade
    {
        private readonly Predicate<Child> isNotAdult;
        private readonly GroupService groupService;
        private readonly UnitOfWork unitOfWork;
        private readonly ChildInDbNullableToChildMapper _fromDbNullableToChildMapper;

        public ChildFacade(UnitOfWork unitOfWork)
        {
            this.unitOfWork = unitOfWork;
            groupService = new GroupService(unitOfWork);
            isNotAdult = child => child.Age < 18;
        }

        public bool AddChild(Child child)
        {
            if (child == null || !isNotAdult.Invoke(child)) return false;
            groupService.AddChildToGroup(child);
            return true;
        }

        public Child FindChild(Guid id)
        {
            return id == Guid.Empty ? null : _fromDbNullableToChildMapper.Map(
                unitOfWork.ChildRepository.FindById(id));
        }

        public List<Child> GetAll()
        {
            return unitOfWork.ChildRepository.FindAll()
                .Select(_fromDbNullableToChildMapper.Map)
                .ToList();
        }
    }
}