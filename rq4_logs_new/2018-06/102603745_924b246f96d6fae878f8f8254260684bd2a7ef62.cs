using System;
using System.Threading.Tasks;
using Atacama.Apenio.NKS.API;
using NksAPI.Atacama.Apenio.NKS.API.IO;

namespace NksAPI.Atacama.Apenio.NKS.API.Test.Apenio
{
    public class Workflow
    {
        private const string Server = "http://apenioapp02:19080";
        
        public static async Task<NksResponse> ACC_000_01()
        {
            SimpleQueryBuilder builder = Nks.NewConnection(Server).PrepareRequest().Search().AdHocIntervention()
                .CreateSimpleQuery()
                .AddTargets()
                .Interventions().AddStructure("AkutPflege_").Done()
                .InterventionsBundle().AddStructure("AkutPflege_").Done()
                .Done()
                .AddConcept("TA2.0").Done()
                .SetDepth(20);
            return await builder.Execute();
        }
        
        public static async Task<NksResponse> ACC_000_02()
        {
            SimpleQueryBuilder builder = Nks.NewConnection(Server).PrepareRequest().Search().AdHocIntervention()
                .CreateSimpleQuery()
                .AddTargets()
                    .Interventions().AddStructure("AkutPflege_").Done()
                    .InterventionsBundle().AddStructure("AkutPflege_").Done()
                .Done()
                .AddConcept("PC1115").Done()
                .AddConcept("TA2.0").Done()
                .AddConcept("UA0").Done()
                .AddConcept("BA293").Done()
                .AddConcept("PA50").Done()
                .SetDepth(10);
            Console.Out.WriteLine(builder.GetPath());
            new NksJson().Display(builder.GetQuery());
            return await builder.Execute();
        }
        
        public static async Task<NksResponse> ACC_000_03()
        {
            SimpleQueryBuilder builder = Nks.NewConnection(Server).PrepareRequest().Search().Proposal()
                .CreateSimpleQuery()
                .AddTargets()
                .Interventions().AddStructure("AkutPflege_").Done()
                .InterventionsBundle().AddStructure("AkutPflege_").Done()
                .Done()
                .SetDepth(7)
                .SetSearchText("Verba");
            Console.Out.WriteLine(builder.GetPath());
            new NksJson().Display(builder.GetQuery());
            return await builder.Execute();
        }
        
        public static async Task<NksResponse> ACC_000_04()
        {
            SimpleQueryBuilder builder = Nks.NewConnection(Server).PrepareRequest().Search().Proposal()
                .CreateSimpleQuery()
                .AddTargets()
                .Causes().AddStructure("AkutPflege_").Done()
                .Done()
                .SetSearchText("Oberfl")
                .SetDepth(6);
            return await builder.Execute();
        }
        
        public static async Task<NksResponse> ACC_000_05()
        {
            SimpleQueryBuilder builder = Nks.NewConnection(Server).PrepareRequest().Search().Catalog()
                .CreateSimpleQuery()
                .AddTargets()
                .Causes().AddStructure("AkutPflege_").Done()
                .Done()
                .AddAttributes()
                .Aged()
                .Female()
                .Done()
                .AddConcept("TA11.0").Done()
                .SetSearchText("Oberfl")
                .SetDepth(10);
            //Console.Out.WriteLine(builder.GetPath());
            //new NksJson().Display(builder.GetQuery());
            return await builder.Execute();
        }
        
        public static async Task<NksResponse> ACC_000_06()
        {
            SimpleQueryBuilder builder = Nks.NewConnection(Server).PrepareRequest().Search().Catalog()
                .CreateSimpleQuery()
                .AddTargets()
                    .Interventions().AddStructure("AkutPflege_").Done()
                    .InterventionsBundle().AddStructure("AkutPflege_").Done()
                .Done()
                .AddAttributes()
                    .Aged()
                    .Female()
                .Done()
                .AddConcept("TA11.0").Done()
                .SetSearchText("Oberfl")
                .SetDepth(20);
            
            new NksJson().Display(builder.GetQuery());
            return await builder.Execute();
        }
        
        public static async Task<NksResponse> ACC_000_07()
        {
            SimpleQueryBuilder builder = Nks.NewConnection(Server).PrepareRequest().Access().Element()
                .CreateSimpleQuery()
                .AddTargets()
                    .BodyLocations().AddStructure("AkutPflege_").Done()
                    .BodyLocationsStructure().AddStructure("AkutPflege_").Done()
                .Done()
                .AddConcept("BF75").Done()
                .SetOrder().List()
                .SetDepth(1);
            new NksJson().Display(builder.GetQuery());
            return await builder.Execute();
        }
        
        public static async Task<NksResponse> ACC_000_08()
        {
            SimpleQueryBuilder builder = Nks.NewConnection(Server).PrepareRequest().Search().Proposal()
                .CreateSimpleQuery()
                .AddTargets()
                .BodyLocations().AddStructure("AkutPflege_").Done()
                .BodyLocationsStructure().AddStructure("AkutPflege_").Done()
                .Done()
                .SetLanguage("la")
                .SetDepth(10)
                .SetSearchText("Arm");
            Console.Out.WriteLine(builder.GetPath());
            new NksJson().Display(builder.GetQuery());
            return await builder.Execute();
        }
    }
}