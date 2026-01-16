using HtmlAgilityPack;
using Newtonsoft.Json;
using OfficeOpenXml.FormulaParsing.Excel.Functions.Math;
using Polly;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Thompson.RecordSearch.Utility.Classes;
using Thompson.RecordSearch.Utility.Dto;
using Thompson.RecordSearch.Utility.Extensions;

namespace LegalLead.PublicData.Search.Util
{
    using dst = System.Net.Cookie;
    using Rx = Properties.Resources;
    using src = OpenQA.Selenium.Cookie;
    public class DalllasBulkCaseReader : BaseDallasSearchAction
    {
        public override int OrderId => 80;
        public IWebInteractive Interactive { get; set; }
        public List<CaseItemDto> Workload { get; set; }

        public override object Execute()
        {
            if (Parameters == null || Driver == null)
                throw new NullReferenceException(Rx.ERR_DRIVER_UNAVAILABLE);

            if (Workload.Count == 0)
                throw new NullReferenceException(Rx.ERR_URI_MISSING);
            var cookies = Driver.Manage().Cookies.AllCookies;
            if (cookies.Count > 0)
            {
                Debug.WriteLine("Found {0:d} cookies", cookies.Count);
            }
            var count = Workload.Count;
            var collection = new Dictionary<int, string>();
            Parallel.ForEach(Workload,
                new ParallelOptions() { MaxDegreeOfParallelism = 5 },
            c =>
                {

                    var idx = Workload.IndexOf(c);
                    var current = collection.Count;
                    EchoIteration(c, current, count);
                    var content = GetPageAsync(c.Href, cookies).GetAwaiter().GetResult();
                    collection[idx] = content;
                    if (!string.IsNullOrEmpty(content))
                    {
                        var data = GetPageContent(content);
                        collection[idx] = data;
                    }
                });
            return collection;
        }

        private void EchoIteration(CaseItemDto dto, int current, int max)
        {
            if (Interactive == null) return;
            var d1 = dto.FileDate;
            var dateformat = CultureInfo.CurrentCulture.DateTimeFormat;
            if (DateTime.TryParse(d1, dateformat, DateTimeStyles.AssumeLocal, out var dte1)) d1 = dte1.ToString("M/d", dateformat);
            Interactive.EchoProgess(0, max, current, $"{d1} - Reading item {current} of {max}.", true, dateNotification: d1);
        }
        private static async Task<string> GetPageAsync(string uri, IEnumerable<src> cookies)
        {
            var baseAddress = new Uri(uri);
            var cookieContainer = new CookieContainer();

            var cookieJar = cookies.ToList();
            var cookieCollection = new CookieCollection();
            foreach (var cookie in cookieJar)
            {
                if (string.IsNullOrEmpty(cookie.Value.Trim())) continue;
                cookieCollection.Add(new dst(cookie.Name, cookie.Value));
            }
            cookieContainer.Add(baseAddress, cookieCollection);
            var policy = Policy<string>
                .HandleResult(response => response.Equals("error"))
                .WaitAndRetryAsync(5, retryAttempt => {
                    var ms = 300 * Math.Pow(2, retryAttempt);
                    return TimeSpan.FromMilliseconds(ms);
                    });

            var result = await policy.ExecuteAsync(() => GetRemotePageAsync(baseAddress, cookieContainer));
            return result;
        }

        private static async Task<string> GetRemotePageAsync(Uri baseAddress, CookieContainer cookieContainer)
        {
            try
            {
                using var handler = new HttpClientHandler() { CookieContainer = cookieContainer };
                using var client = new HttpClient(handler) { Timeout = TimeSpan.FromMilliseconds(1200) };
                var result = await client.GetAsync(baseAddress);
                if (result.IsSuccessStatusCode)
                {
                    var contents = await result.Content.ReadAsStringAsync();
                    return contents;
                }
                return "";
            }
            catch (Exception)
            {
                return "error";
            }
        }

        private static string GetPageContent(string html)
        {

            var doc = new HtmlDocument();
            doc.LoadHtml(html);
            var node = doc.DocumentNode;
            var dv = node.SelectSingleNode("//*[@id='divCaseInformation_body']");
            if (dv == null) { return string.Empty; }
            var dvs = dv.SelectNodes("div");
            if (dvs == null || dvs.Count < 2) { return string.Empty; }
            dv = dvs[2];
            if (string.IsNullOrEmpty(dv.InnerText)) return string.Empty;
            var arr = dv.InnerText.Trim().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            if (arr.Length < 2) return string.Empty;
            var cs = arr[1].Trim();
            /* get plaintiff */
            var list = node.SelectNodes("//span").ToList();
            var find = list.FindAll(a =>
            {
                var attr = a.GetAttributeValue("class", "null");
                if (attr == "null") { return false; }
                return attr == "text-primary";
            })
                .FindAll(b => b.InnerText.Trim() == "PLAINTIFF");
            if (find == null || find.Count == 0) { return string.Empty; }
            var pln = find[0];
            var paragraph = Closest(pln, "p");
            if (paragraph == null) { return string.Empty; }
            Console.WriteLine("Reading case: {0}", cs);
            var obj = new TheAddress
            {
                CaseStyle = cs,
                Plaintiff = paragraph.InnerText.Replace("PLAINTIFF", "").Trim(),
                Address = ""
            };
            /* get defendant address */
            var dvparty = node.SelectSingleNode("//*[@id='divPartyInformation_body']");
            var partytypes = new List<string> { "RESPONDENT", "DEFENDANT" };
            if (dvparty == null) return obj.ToJsonString();
            var children = dvparty.SelectNodes("//p")?.ToList();
            if (children == null || children.Count == 0) { return obj.ToJsonString(); }
            var parties = children.FindAll(f =>
            {
                var found = false;
                var dvp = Closest(f, "div");
                if (dvp == null) { return false; }
                var txt = dvp?.InnerText?.Trim() ?? string.Empty;
                partytypes.ForEach(p => { if (txt.IndexOf(p) >= 0) { found = true; } });
                if (!found) { return false; }
                return f.InnerHtml.IndexOf("<span") < 0;
            }).ToList();
            if (!parties.Any()) return obj.ToJsonString();
            var addr = parties[0].InnerText.Trim();
            addr = addr.Replace(Environment.NewLine, "|").Trim();
            while (addr.IndexOf("||") >= 0) { addr = addr.Replace("||", "|").Trim(); }
            obj.Address = addr;
            return obj.ToJsonString();
        }
        private sealed class TheAddress
        {
            [JsonProperty("addr")] public string Address { get; set; }
            [JsonProperty("plaintiff")] public string Plaintiff { get; set; }
            [JsonProperty("casestyle")] public string CaseStyle { get; set; }
        }
        private static HtmlNode Closest(HtmlNode node, string elementName)
        {
            if (node == null) { return null; }
            var parent = node.ParentNode;
            while (parent != null && parent.Name.ToLower() != elementName)
            {
                parent = parent.ParentNode;
            }
            return parent;
        }
    }
}