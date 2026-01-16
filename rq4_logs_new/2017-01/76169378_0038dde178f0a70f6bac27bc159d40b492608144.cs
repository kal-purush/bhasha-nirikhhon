using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Workflow_Models;
using Workflow_Models.Models;

namespace Workflow_BL.BSL
{
    class AdminService
    {
        private static DatabaseConfiguration context;

        public AdminService(DatabaseConfiguration context)
        {
            AdminService.context = context;
        }

        public static IEnumerable<UserLog> GetAllLogs()
        {
            return new UserLogRepository(context).GetAllLogs();
        }

        public static IEnumerable<UserLog> GetLogsUserType(Permission type)
        {
            if(type.Equals(Permission.Admin))
                return new UserLogRepository(context).GetLogByUserAdmin();

            if (type.Equals(Permission.Manager))
                return new UserLogRepository(context).GetLogByUserManager();

            if (type.Equals(Permission.Contributor))
                return new UserLogRepository(context).GetLogByUserContributor();

            return new UserLogRepository(context).GetLogByUserSimpleUser();
        }

        public static IEnumerable<UserLog> GetLogsByInterval(string start, string end)
        {
            return new UserLogRepository(context).GetLogByInterval(start, end);
        }

        public static IEnumerable<Document> GetDocumentsByKeyword(string keyword)
        {
            return new DocumentRepository(context).GetDocumentsByKeyword(keyword);
        }
    }
}