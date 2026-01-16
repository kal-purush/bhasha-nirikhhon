using Remoteit.Models;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("Remoteit.Test")]

namespace Remoteit.RestApi
{
    internal interface IRemoteitApiSessionManager
    {
        public HttpClient HttpApiClient { get; set; }

        public RemoteitApiSession CurrentSessionData { get; set; }

        public Task<RemoteitApiSession> GenerateSession(IEnumerable<char> userName, IEnumerable<char> userPassword);

        public bool SessionHasExpired();
    }
}