using StudentAttandanceLibrary.Models;
using StudentAttandanceLibrary.Repositories.IRepositories;

namespace StudentAttandanceLibrary.Repositories.Implements
{
    public class GroupRepository : IGroupRepository
    {
        StudentAttendanceManagementContext context = new StudentAttendanceManagementContext();

        public IQueryable<Group> GetGroupsByTermAndCourse(int termId, int courseId)
        {
            var query = context.Groups.Where(x => x.TermId == termId && x.CourseId == courseId);
            return query;
        }
    }
}