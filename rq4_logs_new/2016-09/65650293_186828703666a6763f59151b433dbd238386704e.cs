using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dyllan.Common.Web;
using Dyllan.Common.Utility;

namespace GetGithub
{
    public abstract class AbstractExecutor : IExecute
    {
        private volatile bool _executed = false;
        protected readonly string _url;
        protected Project _project;
        public string Url
        {
            get
            {
                return _url;
            }
        }

        protected static Logger _log = new Logger();

        public virtual void Execute()
        {
            if (_executed)
                return;

            lock (this)
            {
                if (_executed)
                    return;
                try
                {
                    _htmlContent = _httpHelper.GetURL(_url);
                }
                catch (Exception ex)
                {
                    _log.Log(_url);
                    _log.Log(ex);
                }
            }
        }

        public AbstractExecutor(string url, Project project)
        {
            _url = url;
            _project = project;
        }

        protected static HttpHelper _httpHelper = new HttpHelper();
        protected string _htmlContent;
    }
}