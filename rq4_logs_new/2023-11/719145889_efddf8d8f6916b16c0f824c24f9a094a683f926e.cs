using System.Globalization;

namespace Harris.Criminal.Db
{
    public static class CaseStyleDbInspector
    {
        private static CultureInfo GetCulture => CultureInfo.InvariantCulture;
        private static StringComparison Oic => StringComparison.OrdinalIgnoreCase;

        public static bool HasHeader(DateTime filingDate)
        {
            var db = Startup.CaseStyles.DataList;
            var fileDate = filingDate.ToString("M/d/yyyy", GetCulture);
            return db.Any(f => f.FileDate.Equals(fileDate, Oic));
        }

        public static bool HasDetail(DateTime filingDate)
        {
            if (!HasHeader(filingDate))
            {
                return false;
            }
            var db = Startup.Downloads.DataList;
            var fileDate = filingDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
            foreach (var dataset in db)
            {
                var found = dataset.Data.Any(a => a.FilingDate.Equals(fileDate, Oic));
                if (!found)
                {
                    continue;
                }

                found = dataset.Data.Any(a =>
                    a.FilingDate.Equals(fileDate, Oic));
                if (found)
                {
                    return true;
                }
            }
            return false;
        }

        public static bool HasDetail(DateTime filingDate, string caseNumber)
        {
            if (!HasDetail(filingDate))
            {
                return false;
            }
            var db = Startup.Downloads.DataList;
            var fileDate = filingDate.ToString("M/d/yyyy", CultureInfo.InvariantCulture);
            foreach (var dataset in db)
            {
                var found = dataset.Data.Any(a => a.FilingDate.Equals(fileDate, Oic));
                if (!found)
                {
                    continue;
                }

                found = dataset.Data.Any(a =>
                    a.FilingDate.Equals(fileDate, Oic) &&
                    a.CaseNumber.Equals(caseNumber, Oic));
                if (found)
                {
                    return true;
                }
            }
            return false;
        }
    }
}