using Entities;

namespace Http
{
    internal class HttpProtocol : ListenerProtocol
    {
        public override string Accept => throw new System.NotImplementedException();

        public override string EOF => throw new System.NotImplementedException();

        public override string Exit => throw new System.NotImplementedException();

        protected override string Greetings => throw new System.NotImplementedException();

        protected override string Goodbye => throw new System.NotImplementedException();

        protected override string GetNext(string req)
        {
            throw new System.NotImplementedException();
        }
    }
}