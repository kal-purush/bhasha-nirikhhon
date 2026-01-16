using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WcfFynbusService.Entity_Framework.Implemented
{
    public class RdgBasicInformation : Implemented.RdgBase
    {

        public RdgBasicInformation()
            : base()
        {

        }

        ~RdgBasicInformation()
        {
            this.dbContext.Dispose();
        }

        public bool Add(string name, int cvr, string nameSecondary)
        {
            try
            {
                var obj = new BasicInformation();
                obj.Name = name;
                obj.CVR = cvr;
                obj.NameSecondary = nameSecondary;

                this.dbContext.BasicInformations.Add(obj);
                this.dbContext.SaveChanges();
            }
            catch (Exception)
            {
                return false;
            }

            return true;
        }

        public bool Update(int id, string name, int cvr, string nameSecondary)
        {
            try
            {
                var obj = this.dbContext.BasicInformations.SingleOrDefault(x => x.ID == id);

                obj.Name = name;
                obj.CVR = cvr;
                obj.NameSecondary = nameSecondary;

                this.dbContext.SaveChanges();
            }
            catch (Exception)
            {
                return false;
            }

            return true;
        }

        public bool Remove(int id)
        {
            try
            {
                this.dbContext.BasicInformations.Remove(
                    this.dbContext.BasicInformations.SingleOrDefault(x => x.ID == id));
            }
            catch (Exception)
            {
                return false;
            }

            return true;
        }

        public BasicInformation Find(int id)
        {
            try
            {
                return this.dbContext.BasicInformations.SingleOrDefault(x => x.ID == id);
            }
            catch (Exception)
            {
                return null;
            }
        }

        public int? FindID(string name, int cvr, string nameSecondary)
        {
            try
            {
                return this.dbContext.BasicInformations.SingleOrDefault(
                    x => x.Name == name & x.CVR == cvr & x.NameSecondary == nameSecondary).ID;
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}