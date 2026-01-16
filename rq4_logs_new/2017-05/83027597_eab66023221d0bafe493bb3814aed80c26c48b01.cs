using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Driver.Builders;
using MongoDB.Driver.Linq;

namespace Sah
{
    class Baza
    {
        public void dodaj(string s1, string s2)
        {
            Partija p = new Partija();
            var connectionString = "mongodb://localhost/?safe=true";
            var server = MongoServer.Create(connectionString);
            var database = server.GetDatabase("nesto1");
            var collection = database.GetCollection("ggg");
            //string ss = s;
            p.pozicija = s2;
            p.ime = s1;
            try
            {
                collection.Insert(p);
            }
            catch
            {
               // MessageBox.Show("Doslo je do greske!!!");
            }
        }

        public string ucitaj(string s)
        {
            var connectionString = "mongodb://localhost/?safe=true";
            var server = MongoServer.Create(connectionString);
            var db = server.GetDatabase("nesto1");
            var collection = db.GetCollection<Partija>("ggg");
            var query = Query.EQ("ime", s);
            foreach (Partija pa in collection.Find(query))
            {
                s=pa.pozicija;
            }
            return s;
        }

        public void dodajAutomatski(string s2)
        {
            Partija p = new Partija();
            var connectionString = "mongodb://localhost/?safe=true";
            var server = MongoServer.Create(connectionString);
            var db = server.GetDatabase("nesto1");
            try
            {
                var collection = db.GetCollection<Partija>("ggg");
                var query = Query.EQ("ime", "prekinuta");
                collection.Remove(query);
                p.pozicija = s2;
                p.ime = "prekinuta";
                collection.Insert(p);
            }
            catch
            {
                //MessageBox.Show("Doslo je do greske!");
            }
        }
    }
}