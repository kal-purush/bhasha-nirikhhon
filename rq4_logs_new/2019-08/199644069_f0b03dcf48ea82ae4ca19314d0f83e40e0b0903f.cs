using Abis.Mbs.Business.Abstract;
using Abis.Mbs.DataAccess.Abstract;
using Abis.Mbs.Entities.Concrete;
using System;
using System.Collections.Generic;
using System.Text;

namespace Abis.Mbs.Business.Concrete
{
    public class JobManager : IJobService
    {
        private IJobDal _jobDal;

        public JobManager(IJobDal jobDal)
        {
            _jobDal = jobDal;
        }
        public void Add(Job job)
        {
            _jobDal.Add(job);
        }

        public void Delete(int jobID)
        {
            _jobDal.Delete(new Job { JobID = jobID });
        }

        public List<Job> GetAll()
        {
            return _jobDal.GetList();
        }

        public Job GetById(int jobID)
        {
            return _jobDal.Get(p => p.JobID == jobID);
        }

        public void Update(Job job)
        {
            _jobDal.Update(job);
        }
    }
}