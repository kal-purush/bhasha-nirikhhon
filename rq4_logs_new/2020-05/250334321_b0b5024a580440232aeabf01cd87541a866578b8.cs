using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Filters;
using SchedulearnBackend.UserFriendlyExceptions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace SchedulearnBackend.Middleware
{
    public class TransactionFilter
    {
        private readonly RequestDelegate next;
        public TransactionFilter(RequestDelegate next)
        {
            this.next = next;
        }

        public async Task Invoke(HttpContext context, DbTransaction transaction)
        {
            try
            {
                var connection = transaction.Connection;
                if (connection.State != ConnectionState.Open)
                    throw new UserFriendlyException(HttpStatusCode.InternalServerError, "It seems our servers are down right now");

                await next(context);
                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
    }
}