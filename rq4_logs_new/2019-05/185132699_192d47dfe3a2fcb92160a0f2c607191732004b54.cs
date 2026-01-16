using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace o2o.entity
{
    public class Requirement
    {
        private int id;

        public int Id
        {
            get { return id; }
            set { id = value; }
        }

        private String requirementName;

        public String RequirementName
        {
            get { return requirementName; }
            set { requirementName = value; }
        }

        private String requirementDesc;

        public String RequirementDesc
        {
            get { return requirementDesc; }
            set { requirementDesc = value; }
        }

        private String requirementImg;

        public String RequirementImg
        {
            get { return requirementImg; }
            set { requirementImg = value; }
        }

        private String requirementDescImg;

        public String RequirementDescImg
        {
            get { return requirementDescImg; }
            set { requirementDescImg = value; }
        }

        private int priority;

        public int Priority
        {
            get { return priority; }
            set { priority = value; }
        }

        private User userId;

        public User UserId
        {
            get { return userId; }
            set { userId = value; }
        }
        private RequirementCategory requirementCategory;

        public RequirementCategory RequirementCategory
        {
            get { return requirementCategory; }
            set { requirementCategory = value; }
        }

        private DateTime createTime;

        public DateTime CreateTime
        {
            get { return createTime; }
            set { createTime = value; }
        }

        private DateTime modifyTime;

        public DateTime ModifyTime
        {
            get { return modifyTime; }
            set { modifyTime = value; }
        }

        private int requirementStatus;

        public int RequirementStatus
        {
            get { return requirementStatus; }
            set { requirementStatus = value; }
        }

    }
}