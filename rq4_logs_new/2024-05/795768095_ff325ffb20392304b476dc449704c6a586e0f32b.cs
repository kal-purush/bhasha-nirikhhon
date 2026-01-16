using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public abstract class BaseHandler : IHandler
    {
        private IHandler nextHandler;
        public IHandler SetNext(IHandler handler)
        {
            nextHandler = handler;

            return handler;
        }

        public virtual string Handle(string request)
        {
            if (nextHandler != null)
            {
                return nextHandler.Handle(request);
            }
            else
            {
                return "No handler to use. Retry please";
            }
        }
    }
}