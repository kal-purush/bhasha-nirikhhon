using System.Collections.Generic;
using CodeStash3.BLL;
using System.Web.Script.Serialization;
using System.IO;
using CodeStash3.DAL.Properties;

namespace CodeStash3.DAL
{
    public class JsonSnippetRepository : ISnippetRepository
    {

        string dbFileName = Settings.Default.jsonDB;

        public List<Snippet> GetAllSnippets()
        {
            string snippetStr = File.ReadAllText(dbFileName);
            List<Snippet> snippets = new JavaScriptSerializer().Deserialize<List<Snippet>>(snippetStr);
            return snippets;
        }

        

        public void UpdateAllSnippets(List<Snippet> snippets)
        {
            string json = new JavaScriptSerializer().Serialize(snippets);
            File.WriteAllText(dbFileName, json);
        }



        //GetSnippet(int id)
        //DeleteSnippet(int id)
        //DeleteSnippet(Snippet snippet);
        //SaveSnippet(Snippet snippet); //create if not exists; or update if exists
    }
}