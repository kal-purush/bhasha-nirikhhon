using TennisClub.AppCore.interfaces;
using TennisClub.AppCore.model.impl;
using TennisClub.AppCore.model.interfaces;
using TennisClub.AppCore.validators;
using TennisClub.Data.dao;
using TennisClub.Infrastructure.services;

namespace TennisClub.Infrastructure.pipelines
{
    public class ChildPipeline
    {
        private readonly IValidator<IChild> isChildValidator;
        private readonly GroupService groupService;

        public ChildPipeline(DaoObject dao)
        {
            groupService = new GroupService(dao);
            isChildValidator = new IsChildValidator();
        }

        public bool AddChild(Child child)
        {
            if (isChildValidator.Validate(child))
            {
                groupService.AddChildToGroup(child);
                return true;
            }
            return false;
        }
    }
}