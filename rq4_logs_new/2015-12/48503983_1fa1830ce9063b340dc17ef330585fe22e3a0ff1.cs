using Box.V2;
using Box.V2.Auth;
using Box.V2.Config;
using Box.V2.Converter;
using Box.V2.Extensions;
using Box.V2.Managers;
using Box.V2.Models;
using Box.V2.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Box.V2.Managers
{
    public class BoxEventsManager : BoxResourceManager
    {
        public static Uri EventsEndpointUri = new Uri("https://api.box.com/2.0/events");
        public BoxEventsManager(IBoxConfig config, IBoxService service, IBoxConverter converter, IAuthRepository auth, string asUser = null)
            : base(config, service, converter, auth, asUser)
        { }


        /// <summary>
        /// Use this to get events for a given user. A chunk of event objects is returned for the user based on the parameters passed in. Parameters indicating how many chunks are left as well as the next stream_position are also returned.
        /// </summary>
        /// <param name="stream_position">The location in the event stream at which you want to start receiving events. Can specify special case ‘now’ to get 0 events and the latest stream position for initialization.</param>
        /// <param name="stream_type">Limits the type of events returned: all: returns everything, changes: returns tree changes, sync: returns tree changes only for sync folders</param>
        /// <param name="limit">Limits the number of events returned</param>
        public async Task<BoxCollection<BoxItem>> GetUserEventsAsync(string stream_position = "0", string stream_type = "all", int limit = 100)
        {
            stream_position.ThrowIfNullOrWhiteSpace("stream_position");

            BoxRequest request = new BoxRequest(_config.EventsEndpointUri, string.Format(Constants.ItemsPathString, stream_position))
                .Param("stream_type", stream_type.ToString());

            IBoxResponse<BoxCollection<BoxItem>> response = await ToResponseAsync<BoxCollection<BoxItem>>(request).ConfigureAwait(false);

            return response.ResponseObject;
        }
    }
}