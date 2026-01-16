using System.Collections.Generic;

namespace JRequest.Net
{
    public class JRequestContext
    {

        public string Json { get; set; }
        public JRequestContext()
        {

        }

        public JRequestContext(string json)
        {
            Json = json;
        }
        public string Protocol { get; set; } = Enumerators.Protocol.http.ToString();
        public string Name { get; set; } = "JRequest";
        public List<Request> Requests { get; set; }
        public Configuration Configuration { get; set; }

        public JRequestContext Build()
        {
            return Engine.Build(Json);
        }
        public JRequestContext Build(string json)
        {
            return Engine.Build(json);
        }
        public JRequestContext Run()
        {
            return Engine.Run();
        }
    }
}