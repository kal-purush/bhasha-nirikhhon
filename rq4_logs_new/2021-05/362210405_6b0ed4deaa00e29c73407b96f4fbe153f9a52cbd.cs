using RDFSharp.Model;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ManualTestApp
{
    class Program
    {
        static void Main(string[] args)
        {
             try
             {
                 //Teszt1_1: resource létrehozás helyesuri string-gel
                 //Teszt1_1 Elvárás: Sikeres létrehozás
                 //Teszt1_1 kimenetel: Sikeres létrehozás
                 var donaldduck = new RDFResource("http://www.waltdisney.com/donald_duck");
                 Console.WriteLine(donaldduck);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt1_2: resource létrehozás üres string-gel
                //Teszt1_2 Elvárás: Blank resource létrehozás
                //Teszt1_2 kimenetel: Exception
                var emptystring = new RDFResource("");
                 Console.WriteLine(emptystring);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt1_3: resource létrehozás null étrékkel
                //Teszt1_3 Elvárás: Blank resource létrehozás
                //Teszt1_3 kimenetel: Exception
                var nullresource = new RDFResource(null);
                 Console.WriteLine(nullresource);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt1_4: resource létrehozás paraméter nélkül
                //Teszt1_4 Elvárás: Blank resource létrehozás
                //Teszt1_4 kimenetel: sikeresen létrehozott Blank resource
                var parameterless = new RDFResource();
                 Console.WriteLine(parameterless);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt1_5: resource létrehozás nem uri formájú string-gel
                //Teszt1_5 Elvárás: Exception
                //Teszt1_5 kimenetel: Exception
                var noturistring = new RDFResource("noturi");
                 Console.WriteLine(noturistring);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {

                //Teszt2_1: RDFPlainLiteral létrehozás egyszerű srting-gel
                //Teszt2_1 Elvárás: Sikeres létrehozás
                //Teszt2_1 kimenetel: Sikeres létrehozás
                var donaldduck_name = new RDFPlainLiteral("Donald Duck");
                Console.WriteLine(donaldduck_name);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt2_2: RDFPlainLiteral létrehozás üres srting-gel
                //Teszt2_2 Elvárás: Sikeres létrehozás üres srting értékkel
                //Teszt2_2 kimenetel: Sikeres létrehozás
                var blankstring = new RDFPlainLiteral("");
                 Console.WriteLine(blankstring);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt2_3: RDFPlainLiteral létrehozás null értékkel
                //Teszt2_3 Elvárás: Sikeres létrehozás
                //Teszt2_3 kimenetel: Sikeres létrehozás
                var nullliteral = new RDFPlainLiteral(null);
                Console.WriteLine(nullliteral);

            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {

                //Teszt2_4: RDFPlainLiteral létrehozás egyszerű srting-gel és languag tag-gel
                //Teszt2_4 Elvárás: Sikeres létrehozás
                //Teszt2_4 kimenetel: Sikeres létrehozás
                var donaldduck_name_enus = new RDFPlainLiteral("Donald Duck", "en-US");
                Console.WriteLine(donaldduck_name_enus);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt2_5: RDFPlainLiteral létrehozás egyszerű srting-gel és üres languag tag-gel
                //Teszt2_5 Elvárás: Sikeres létrehozás üres language tag-gel
                //Teszt2_5 kimenetel: Sikeres létrehozás üres language tag-gel
                var donaldduck_name_empty = new RDFPlainLiteral("Donald Duck", "");
                Console.WriteLine(donaldduck_name_empty);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt2_6: RDFPlainLiteral létrehozás egyszerű srting-gel és null languag tag-gel
                //Teszt2_6 Elvárás: Sikeres létrehozás üres language tag-gel
                //Teszt2_6 kimenetel: Sikeres létrehozás üres language tag-gel
                var donaldduck_name_null = new RDFPlainLiteral("Donald Duck", null);
                Console.WriteLine(donaldduck_name_null);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt2_7: RDFPlainLiteral létrehozás null értékkel és null language tag-gel
                //Teszt2_7 Elvárás: Sikeres létrehozás üres értékkel és üres language tag-gel
                //Teszt2_7 kimenetel: Sikeres létrehozás üres értékkel és üres language tag-gel
                var null_null = new RDFPlainLiteral(null, null);
                Console.WriteLine(null_null);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {

                //Teszt3_1: RDFTypedLiteral létrehozás megfelelő értékkel
                //Teszt3_1 Elvárás: Sikeres létrehozás
                //Teszt3_1 kimenetel: Sikeres létrehozás
                var mickeymouse_age = new RDFTypedLiteral("85", RDFModelEnums.RDFDatatypes.XSD_INTEGER);
                Console.WriteLine(mickeymouse_age);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt3_2: RDFTypedLiteral létrehozás null értékkel
                //Teszt3_2 Elvárás: Exception
                //Teszt3_2 kimenetel: Exception
                var mickeymouse_null = new RDFTypedLiteral(null, RDFModelEnums.RDFDatatypes.XSD_NAME);
                Console.WriteLine(mickeymouse_null);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt3_3: RDFTypedLiteral létrehozás üres értékkel
                //Teszt3_3 Elvárás: Exception
                //Teszt3_3 kimenetel: Exception
                var mickeymouse_empty = new RDFTypedLiteral("", RDFModelEnums.RDFDatatypes.XSD_INTEGER);
                Console.WriteLine(mickeymouse_empty);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt3_4: RDFTypedLiteral létrehozás nem megfelelő értékkel
                //Teszt3_4 Elvárás: Exception
                //Teszt3_4 kimenetel: Exception
                var mickeymouse_badvalue = new RDFTypedLiteral("valami", RDFModelEnums.RDFDatatypes.XSD_INTEGER);
                Console.WriteLine(mickeymouse_badvalue);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {

                //Teszt4_1: RDFTriple létrehozás létező elemekkel
                //Teszt4_1 Elvárás: sikeres létrehozás
                //Teszt4_1 kimenetel: sikeres létrehozás
                RDFTriple mickeymouse_is85yr = new RDFTriple(
                new RDFResource("http://www.waltdisney.com/mickey_mouse"),
                new RDFResource("http://xmlns.com/foaf/0.1/age"),
                new RDFTypedLiteral("85", RDFModelEnums.RDFDatatypes.XSD_INTEGER));
                Console.WriteLine(mickeymouse_is85yr);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt4_2: RDFTriple létrehozás blank Resource elemekkel
                //Teszt4_2 Elvárás: exeption
                //Teszt4_2 kimenetel: exeption
                RDFTriple blank_is85blank = new RDFTriple(
                new RDFResource(),
                new RDFResource(),
                new RDFTypedLiteral("85", RDFModelEnums.RDFDatatypes.XSD_INTEGER));
                Console.WriteLine(blank_is85blank);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {

                //Teszt5_1: RDFGraph létrehozás üres konstruktorral
                //Teszt5_1 Elvárás: context-et ad neki
                //Teszt5_1 kimenetel: context-et kapott
                var waltdisney = new RDFGraph();
                Console.WriteLine(waltdisney);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }

            try
            {

                //Teszt5_2: RDFGraph létrehozás RDFTriple listából
                //Teszt5_2 Elvárás: létrehozza a gráfot a megadott RDFTriple listából
                //Teszt5_2 kimenetel: létrehozza a gráfot a megadott RDFTriple listából

                // "Donald Duck has english (US) name "Donald Duck""
                RDFTriple donaldduck_name_enus_triple = new RDFTriple(
                new RDFResource("http://www.waltdisney.com/donald_duck"),
                new RDFResource("http://xmlns.com/foaf/0.1/name"),
                new RDFPlainLiteral("Donald Duck", "en-US"));

                RDFTriple mickeymouse_is85yr = new RDFTriple(
                new RDFResource("http://www.waltdisney.com/mickey_mouse"),
                new RDFResource("http://xmlns.com/foaf/0.1/age"),
                new RDFTypedLiteral("85", RDFModelEnums.RDFDatatypes.XSD_INTEGER));

                var waltdisney_list = new RDFGraph(new List<RDFTriple>() { mickeymouse_is85yr, donaldduck_name_enus_triple });
                foreach(RDFTriple Triple in waltdisney_list)
                Console.WriteLine(Triple);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt5_4:	RDFGraph létrehozás uri-ból:
                //Teszt5_4 Elvárás: Létrehozza a gráfot az uri-ról kapott adatbázisról
                //Teszt5_4 kimenetel: Exeption
                var foafGraph = RDFGraph.FromUri(new Uri("http://bigco.example/"));
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt6_1: DataTable létrehozás RDFGraph-ból, ami tartalmaz Triple elemeket:
                //Teszt6_1 Elvárás: Sikeresen létrehozott Triple-ökből összerakott tábla
                //Teszt6_1 kimenetel: Sikeresen létrehozott Triple-ökből összerakott tábla

                RDFTriple donaldduck_name_enus_triple = new RDFTriple(
                new RDFResource("http://www.waltdisney.com/donald_duck"),
                new RDFResource("http://xmlns.com/foaf/0.1/name"),
                new RDFPlainLiteral("Donald Duck", "en-US"));

                RDFTriple mickeymouse_is85yr = new RDFTriple(
                new RDFResource("http://www.waltdisney.com/mickey_mouse"),
                new RDFResource("http://xmlns.com/foaf/0.1/age"),
                new RDFTypedLiteral("85", RDFModelEnums.RDFDatatypes.XSD_INTEGER));

                var waltdisney_list = new RDFGraph(new List<RDFTriple>() { mickeymouse_is85yr, donaldduck_name_enus_triple });

                var waltdisney_table = waltdisney_list.ToDataTable();
                foreach (DataRow dataRow in waltdisney_table.Rows)
                {
                    foreach (var item in dataRow.ItemArray)
                    {
                        Console.WriteLine(item);
                    }
                }

            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt6_2: DataTable létrehozás RDFGraph-ból, ami nem tartalmaz Triple elemeket:
                //Teszt6_2 Elvárás: Üres tábla létrehozás
                //Teszt6_2 kimenetel: Üres tábla létrehozás.


                var waltdisney = new RDFGraph();

                var waltdisney_table = waltdisney.ToDataTable();
                foreach (DataRow dataRow in waltdisney_table.Rows)
                {
                    foreach (var item in dataRow.ItemArray)
                    {
                        Console.WriteLine(item);
                    }
                }

            }
            catch (Exception e) { Console.WriteLine(e.Message); }
           


            try
            {
                //Teszt7_1: RDFGraph file-ba írása:
                //Teszt7_1 Elvárás: File létrehozás az RDFTriple elemekkel
                //Teszt7_1 kimenetel: File létrehozás az RDFTriple elemekkel
                RDFTriple donaldduck_name_enus_triple = new RDFTriple(
                new RDFResource("http://www.waltdisney.com/donald_duck"),
                new RDFResource("http://xmlns.com/foaf/0.1/name"),
                new RDFPlainLiteral("Donald Duck", "en-US"));

                RDFTriple mickeymouse_is85yr = new RDFTriple(
                new RDFResource("http://www.waltdisney.com/mickey_mouse"),
                new RDFResource("http://xmlns.com/foaf/0.1/age"),
                new RDFTypedLiteral("85", RDFModelEnums.RDFDatatypes.XSD_INTEGER));

                var waltdisney_list = new RDFGraph(new List<RDFTriple>() { mickeymouse_is85yr, donaldduck_name_enus_triple });

                waltdisney_list.ToFile( RDFModelEnums.RDFFormats.NTriples, Directory.GetParent(System.Reflection.Assembly.GetExecutingAssembly().Location)+"/graph.3n");
                
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            try
            {
                //Teszt7_2: RDFGraph file-ból betöltése:
                //Teszt7_2 Elvárás: File-ból létrehozza a RDFGraph-ot az RDFTriple elemekkel
                //Teszt7_2 kimenetel: File-ból létrehozza a RDFGraph-ot az RDFTriple elemekkel
                var waltdisney_list = RDFGraph.FromFile(RDFModelEnums.RDFFormats.NTriples, Directory.GetParent(System.Reflection.Assembly.GetExecutingAssembly().Location) + "/graph.3n");
                foreach (RDFTriple Triple in waltdisney_list)
                    Console.WriteLine(Triple);
            }
            catch (Exception e) { Console.WriteLine(e.Message); }

        }
    }
}