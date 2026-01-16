//#define WRITE_ALL_JSON_TO_DISK

using System.Threading.Tasks;
using Macquarie.Handbook.Data;
using Macquarie.Handbook.Data.Shared;
//Bad separation...

namespace Macquarie.Handbook;

public interface IMacquarieHandbook
{
    Task<string> GetUnitRawJson(string unitCode);
    Task<MacquarieDataCollection<MacquarieCourse>> GetAllCourses(int? implementationYear = null, int limit = 3000);
    Task<MacquarieDataCollection<MacquarieUnit>> GetAllUnits(int? implementationYear = null, int limit = 3000, bool readFromDisk = false);
    Task<MacquarieCourse> GetCourse(string courseCode, int? implementationYear = null, bool tryRetrieveFromLocalCache = true);
    Task<MacquarieUnit> GetUnit(string unitCode, int? implementationYear = null, bool tryRetrieveFromLocalCache = true);
}