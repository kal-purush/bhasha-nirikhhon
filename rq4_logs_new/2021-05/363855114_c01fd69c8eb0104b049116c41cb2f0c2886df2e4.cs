using System;

namespace State
{

    public abstract class BaseState : IState
    {
        protected Context _context;

        public abstract void Handle();

        public void SetContext(Context Context)
        {
            _context = Context;
        }
    }
}