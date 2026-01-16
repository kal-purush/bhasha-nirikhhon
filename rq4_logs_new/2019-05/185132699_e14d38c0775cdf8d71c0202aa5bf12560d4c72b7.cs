using Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Service
{
    public class RequirementService
    {
        RequirementDao requirementDao = new RequirementDao();
        public List<Requirement> getAllRequirement()
        {
            List<Requirement> list = new List<Requirement>();
            list = requirementDao.queryAllRequirement();
            if (list != null)
            {
                return list;
            }
            else
            {
                return null;
            }
        }
        public List<Requirement> getRequirementByCategory(Category category)
        {
            List<Requirement> list = new List<Requirement>();
            list = requirementDao.queryRequirementByCategory();
            if (list != null)
            {
                return list;
            }
            else
            {
                return null;
            }
        }
        public List<Requirement> getRequirementByName(String requirementName)
        {
            List<Requirement> list = new List<Requirement>();
            list = requirementDao.queryRequirementByRequirementName();
            if (list != null)
            {
                return list;
            }
            else
            {
                return null;
            }
        }

    }
}