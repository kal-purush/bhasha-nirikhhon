using InspurOA.DAL;
using InspurOA.Models;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace InspurOA.BLL
{
    public class ProjectBLL
    {
        InspurDbContext db = new InspurDbContext();

        public bool CreateProject(ProjectModel project)
        {
            if (project == null)
            {
                throw new ArgumentNullException("project");
            }

            db.ProjectSet.Add(project);
            int saved = db.SaveChanges();
            return saved > 0;
        }

        public bool UpdateProject(ProjectModel project)
        {
            if (project == null)
            {
                throw new ArgumentNullException("project");
            }

            db.Entry<ProjectModel>(project).State = EntityState.Modified;
            int updated = db.SaveChanges();
            return updated > 0;
        }

        public bool DeleteProject(ProjectModel project)
        {
            if (project == null)
            {
                throw new ArgumentNullException("project");
            }

            db.ProjectSet.Remove(project);
            int deleted = db.SaveChanges();
            return deleted > 0;
        }

        public ProjectModel Find(string id)
        {
            if (string.IsNullOrWhiteSpace(id))
            {
                throw new ArgumentNullException("id");
            }

            return db.ProjectSet.Find(id);
        }

        public IEnumerable<ProjectModel> GetAllProjects()
        {
            return db.ProjectSet.ToList();
        }

        public IQueryable<ProjectModel> QueryResumeByPage(Expression<Func<ProjectModel, bool>> whereSelector, Expression<Func<ProjectModel, string>> KeySelector, out int totalCount, out int pageCount, int offset, int limit = 10)
        {
            totalCount = 0;
            pageCount = 0;
            if (offset < 0)
            {
                throw new ArgumentException("参数不能小于0", "offset");
            }

            if (limit <= 0)
            {
                throw new ArgumentException("参数必须大于0", "limit");
            }

            return db.ProjectSet.QueryByPage<ProjectModel>(whereSelector, KeySelector, out totalCount, out pageCount, offset, limit);
        }
    }
}