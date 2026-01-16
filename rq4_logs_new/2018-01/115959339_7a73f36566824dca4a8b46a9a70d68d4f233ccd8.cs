namespace Entities
{
    public abstract class PortUnit<TIn, TOut>
        where TIn : class
        where TOut : class
    {
        private Port<TIn> _input;
        private Port<TOut> _output;

        protected PortUnit(Port<TIn> input, Port<TOut> output)
        {
            _input = input;
            _output = output;
        }

        public void ConnectTo(Port<TOut> target)
            => _output?.Connect(target);

        public Port<TIn> InputProxy
            => PortProxy<TIn>.LazyWrap(_input);
    }

    internal class PortProxy<T> : Port<T> where T : class
    {
        private PortProxy()
        { }

        public static Port<T> LazyWrap(Port<T> port)
        {
            var proxy = new PortProxy<T>();
            proxy.OnConnected += (s, e) => ((Port<T>)s).Connect(e); 
            return proxy;
        }

        protected override void PostReceive(T data)
        { }

        protected override void PostTransfer(T data)
        { }

        protected override void PreReceive(T data)
        { }

        protected override void PreTransfer(T data)
        { }
    }
}