using MR.Servers;
using MR.Servers.Api;
using MR.Servers.Core.Route.Attributes;
using MR.Servers.Utils;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using VstupInfoParser.Extensions;
using VstupInfoParser.Models_JSON;

namespace VstupInfoParser
{
    [RouteBase("api")]
    internal class Api : CoreAPI
    {
        private readonly CultureInfo ci = new CultureInfo("uk-UA");
        [Route("regions")]
        public ApiResponse GetMainTable()
        {
            return new ApiResponse(new ResponseInfo(
                CoreParser.Current.RegionTable.Serialize()));
        }
        [Route("region/{year}/{name}")]
        public async Task<ApiResponse> GetMainTable(int year, string name)
        {
            var reg = await GetRegion(year, name);

            return new ApiResponse(new ResponseInfo(
                    reg.Institutes.Distinct().Serialize()));
        }
        [Route("instances/{year}/{region}/{namePart}")]
        public async Task<ApiResponse> GetMainTable(int year, string region, string namePart)
        {
            var reg = await GetRegion(year, region);
            var obj = reg.Institutes.Where(x => x.Name.ToLower(ci).Contains(
                           Uri.UnescapeDataString(namePart).ToLower(ci))).Distinct();
            return GetResponse(obj, typeof(InstituteMap));
        }

        [Route("instance/{year}/{region}/{namePart}/{type}")]
        public async Task<ApiResponse> GetForSpecialty
            (int year, string region, string namePart, string type)
        {
            var obj = await GetForSpecialtyQuery(year, region, namePart, type);
            return GetResponse(obj, typeof(SpecialtyMap));
        }

        [Route("instance/{year}/{region}/{namePart}/{type}/{degree}")]
        public async Task<ApiResponse> GetForSpecialtyType
            (int year, string region, string namePart, string type, string degree)
        {
            var p_degree = (Institute.Degree)Enum.Parse(typeof(Institute.Degree), degree);
            var obj = (await GetForSpecialtyQuery(year, region, namePart, type))
                .Where(x => x.Degree == p_degree);
            if (Info.Query.HasKey("faculty"))
            {
                obj = obj.Where(x => x.Faculty != null &&
                x.Faculty.ToLower().Contains(Info.Query["faculty"]));
            }
            if (Info.Query.HasKey("to_files"))
            {
                List<IEnumerable<Students>> list = new List<IEnumerable<Students>>();
                List<string> names = new List<string>();
                foreach (var i in obj)
                {
                    await i.Fetch();
                    names.Add(i.Name + '_' + i.GlobalID);
                    list.Add(i.Students);
                }
                return AsFileResponse(list,names, typeof(StudentsMap));
            }
            else
                return GetResponse(obj, typeof(SpecialtyMap));
        }

        private ApiResponse AsFileResponse<T>(IEnumerable<IEnumerable<T>> obj, IEnumerable<string> names, Type type)
        {
            return new ApiResponse(new ResponseInfo(
                obj.ToCsvFile(names,Server.DirectoryManager[MainApp.Dirs.tmp_csv],  type, "/csv/"),
                "text/plain"));
        }

        [Route("instance/{year}/{region}/{namePart}/{type}/{degree}/{gID}")]
        public async Task<ApiResponse> GetForGlobalID
            (int year, string region, string namePart, string type, string degree, int gID)
        {
            var obj = await GetForSpecialtyQuery(year, region, namePart, type);
            var p_degree = (Institute.Degree)Enum.Parse(typeof(Institute.Degree), degree);
            var q = obj
                .Where(x => x.Degree == p_degree).Where(x => x.GlobalID == gID).FirstOrDefault();
            await q.Fetch();
            return GetResponse(q.Students, typeof(SpecialtyMap));
        }


        private ApiResponse GetResponse<T>(IEnumerable<T> obj, Type type = null)
        {
            var text = obj.ToCsv(type);
            if (Info.Query.HasKey("csv"))
            {
                return new ApiResponse(new ResponseInfo(
                   text, "text/csv"));
            }
            else
                return new ApiResponse(new ResponseInfo(
                      obj.Serialize()));
        }

        private async Task<Region> GetRegion(int year, string name)
        {
            var reg = CoreParser.Current.GetForRegion(year, Uri.UnescapeDataString(name));
            await reg.Fetch();
            return reg;
        }

        private async Task<IEnumerable<Specialty>> GetForSpecialtyQuery
            (int year, string region, string namePart, string type)
        {
            var reg = await GetRegion(year, region);
            var obj = reg.Institutes.Where(x => x.Name.ToLower(ci).Contains(
                           Uri.UnescapeDataString(namePart).ToLower(ci))).Distinct().FirstOrDefault();
            await obj.Fetch();
            var p_type = (Institute.StudyType)Enum.Parse(typeof(Institute.StudyType), type);

            return obj.Specialties[p_type];
        }
    }
}