using CleanArchitecture.Application.Commons.Results;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CleanArchitecture.Domain.Contracts.IServices.IServices
{
    public interface IRestCall
    {

        public QueryOrCommandResult<T> GetRestcall<T>(string uri, string endpoints);

        public QueryOrCommandResult<T> PostRestcall<T>(string uri, string endpoints, object dto);

        public QueryOrCommandResult<T> PutRestcall<T>(string uri, string endpoints, object dto);

        public QueryOrCommandResult<T> DeleteRestcall<T>(string uri, string endpoints, Guid id);
    }
}