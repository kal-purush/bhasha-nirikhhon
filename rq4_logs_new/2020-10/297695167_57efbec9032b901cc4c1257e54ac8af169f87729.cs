using System;
using System.Collections.Generic;
using QuestStore.Core.Entities;

namespace QuestStore.API.GenericControllersFactory
{
    public static class ControllersTypes
    {
        public static Dictionary<Type, ControllerConfiguration> Configurations =>
            new Dictionary<Type, ControllerConfiguration>
            {
                {typeof(Quest), new ControllerConfiguration()},
                {typeof(Artifact), new ControllerConfiguration()},
                {typeof(Classroom), new ControllerConfiguration()},
                {typeof(Student), new ControllerConfiguration()},
                {typeof(Mentor), new ControllerConfiguration()},
                {typeof(User), new ControllerConfiguration()}
            };
    }
}