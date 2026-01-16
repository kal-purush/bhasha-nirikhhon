using install.Interfaces.Basic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using install.Interfaces;

namespace install.Basic
{
    class Controller : ControllerInterface
    {
        private ModelInterface model;

        public Controller(ModelInterface model)
        {
            this.model = model;
        }

        public void initDataBase(SecurityUserInterface admin)
        {
            throw new NotImplementedException();
        }

        public void install()
        {
            throw new NotImplementedException();
        }

        public void setAvevasPath(string path)
        {
            throw new NotImplementedException();
        }

        public void setConnectionString(string connection)
        {
            throw new NotImplementedException();
        }

        public void setIntalledProgramType(bool isParser)
        {
            throw new NotImplementedException();
        }

        public void setLastDate(string date)
        {
            throw new NotImplementedException();
        }

        public void setLogsPath(List<string> logs)
        {
            throw new NotImplementedException();
        }

        public void setProgramPath(string path)
        {
            throw new NotImplementedException();
        }

        public void setTimeModidicatorType(bool isMinute)
        {
            throw new NotImplementedException();
        }

        public void setTimeModificator(int modificator)
        {
            throw new NotImplementedException();
        }
    }
}