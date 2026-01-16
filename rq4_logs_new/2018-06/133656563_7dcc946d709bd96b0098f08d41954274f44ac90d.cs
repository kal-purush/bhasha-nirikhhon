using project2._4.BL.Repositories;
using project2._4.Entities.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Cors;

namespace project2._4.API.Controllers
{
    public class FeedRespectController : ApiController
    {
        [HttpPost]
        [Authorize]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        public IHttpActionResult UpdateRespect(Guid FeedId, bool Upvote)
        {
            FeedRepository db = new FeedRepository();
            TextFeed textFeed = db.GetTextFeed(FeedId);
            ImageFeed ImageFeed = db.GetImageFeed(FeedId);
            VideoFeed VideoFeed = db.GetVideoFeed(FeedId);
            Feed feed = null;
            if (textFeed != null)
            {
                feed = textFeed;
            }
            if (ImageFeed != null)
            {
                feed = ImageFeed;
            }
            if (VideoFeed != null)
            {
                feed = VideoFeed;
            }
            if (Upvote)
            {
                feed.Respect++;
            }
            else
            {
                feed.Respect--;
            }
            if (textFeed != null)
            {
                db.UpdateTextFeed((TextFeed)feed);
            }
            if (ImageFeed != null)
            {
                db.UpdateImageFeed((ImageFeed)feed);
            }
            if (VideoFeed != null)
            {
                db.UpdateVideoFeed((VideoFeed)feed);
            }

            return Ok();
        }
    }
}