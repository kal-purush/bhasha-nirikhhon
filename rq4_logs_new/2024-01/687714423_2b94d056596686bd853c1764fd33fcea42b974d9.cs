using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Microsoft.SqlServer.Types;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Tlv.Search.models;

namespace Tlv.Search
{
    public class Search
    {
        private readonly ILogger<Search>? _logger;

        public Search(ILogger<Search> log)
        {
            _logger = log;
        }

        SearchContext? analyzePrompt(ref string prompt)
        {
            SearchContext sc = new();

            string? connStr = Environment.GetEnvironmentVariable("CuriosityDB");

            try
            {
                using SqlConnection? conn = new(connStr);
                conn.Open();

                SqlCommand? command = new("select * from regions order by tokens_num desc", conn);
                using SqlDataAdapter? da = new();
                da.SelectCommand = command;

                DataSet? ds = new();
                da.Fill(ds);

                bool contextFound = false;
                DataTable? dt = ds.Tables[0];
                foreach (DataRow row in dt.Rows)
                {
                    if (contextFound)
                        break;

                    string names = (string)row["names"];
                    string[] tokens = names.Split(",");
                    for (int i = 0; i < tokens.Length; i++)
                    {
                        var index = prompt.IndexOf(tokens[i]);
                        if (index >= 0)
                        {
                            sc.type = ContextType.Geography;
                            SqlGeography geo = (SqlGeography)row["polygon"];
                            _logger.LogInformation(geo.ToString());
                            sc.geo = geo;
                            sc.name = tokens[i].Trim();
                            contextFound = true;

                            prompt = prompt.Remove(index - 1/*preposition*/ - 1/*space*/,
                                                 tokens[i].Length + 1/*preposition*/ + 1/*space*/);

                            break;
                        }
                    }
                }

                return sc;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
            }

            return null;
        }

        [FunctionName(nameof(Search))]
        [OpenApiOperation(operationId: "Run", tags: new[] { "q" })]
        [OpenApiParameter(name: "q", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "The **prompt** parameter")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(string), Description = "The OK response")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req)
        {

            string? prompt = req.Query["q"];
            _logger.LogInformation($"Running search with prompt '{prompt}'");

            SearchContext? sc = analyzePrompt(ref prompt);

            //string? providerKey = Environment.GetEnvironmentVariable("OPENAI_KEY");
            ////Azure OpenAI package
            //OpenAIClient client = new(providerKey);
            //Response<Embeddings>? response =
            //    client.GetEmbeddings("text-embedding-ada-002",
            //                         new EmbeddingsOptions(prompt)
            //                         );
            //var _embedding = response.Value.Data[0].Embedding;

            string? connStr = Environment.GetEnvironmentVariable("CuriosityDB");
            try
            {
                using (SqlConnection? conn = new(connStr))
                {
                    conn.Open();

                    SqlCommand? command = new("[dbo].[find_Nearest]", conn)
                    {
                        CommandType = CommandType.StoredProcedure
                    };
                    List<SearchItem>? searchItems = new();

                    command.Parameters.Add("@inputText", SqlDbType.NVarChar, -1).Value
                        = prompt;
                    command.Parameters.Add("@top", SqlDbType.Int).Value = 5;

                    if (sc != null)
                    {
                        SqlParameter p = new()
                        {
                            ParameterName = "@inRegion",
                            Value = sc.geo,
                            SqlDbType = SqlDbType.Udt,
                            UdtTypeName = "geography"
                        };
                        command.Parameters.Add(p);
                    }

                    using (SqlDataAdapter? da = new())
                    {
                        da.SelectCommand = command;
                        da.SelectCommand.CommandType = CommandType.StoredProcedure;

                        DataSet? ds = new();

                        Stopwatch sw = new();
                        sw.Start();

                        da.Fill(ds, "result_name");

                        sw.Stop();
                        Console.WriteLine(@$"SP executed for {sw.ElapsedMilliseconds} ms.");

                        DataTable? dt = ds.Tables["result_name"];
                        if (dt != null)
                        {
                            foreach (DataRow row in dt.Rows)
                            {
                                searchItems.Add(new SearchItem()
                                {
                                    id = (ulong)row[0],
                                    title = (string)row[1],
                                    url = (string)row[2],
                                    imageUrl = (string)row[3]
                                    //distance = (double)row[4]
                                });
                            }
                        }
                    }

                    return new JsonResult(searchItems)
                    {
                        StatusCode = (int)HttpStatusCode.OK,
                        ContentType = "application/json"
                    };
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }

        }
    }
}
