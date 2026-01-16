using System.Collections.Generic;

namespace AdapterPattern
{
    public class Adapter : ITarget
    {
        private readonly Adaptee _adaptee;

        public Adapter(Adaptee adaptee)
        {
            _adaptee = adaptee;
        }

        public IDictionary<string, string> Request()
        {
            var response = _adaptee.SpecificRequest();

            return new Dictionary<string, string>
            {
                {"name", response["name"]},
                {"state", response["status"]}
            };
        }
    }
}