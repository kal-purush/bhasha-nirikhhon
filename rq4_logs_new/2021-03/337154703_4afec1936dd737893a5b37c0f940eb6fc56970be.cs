using System;
using TennisClub.AppCore.model.interfaces;

namespace TennisClub.Console.test
{
    public class DaoPrinter
    {
        public void PrintChild(IChild<Guid> child)
        {
            System.Console.WriteLine($"{child.FirstName}, {child.LastName}, {child.Age.ToString()}");
        }

        public void PrintGroup(IGroup<Guid> group)
        {
            System.Console.WriteLine($"{group.GameLevel.ToString()}, {group.LessonsDay.ToString()} "); 
        }
        
        public void PrintCachedGroup(ICachedGroup<Guid> group)
        {
            System.Console.WriteLine($"{group.ChildrenAmount.ToString()}, {group.MinAge.ToString()}, {group.MaxAge.ToString()} {group.Group.GameLevel} {group.Group.LessonsDay} "); 
        }
    }
}