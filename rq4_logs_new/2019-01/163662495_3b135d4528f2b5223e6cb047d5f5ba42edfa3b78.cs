using CommunityNetwork.Common;
using CommunityNetwork.Common.Models;
using CommunityNetWork.Common.Enums;
using Social.BL.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace SocialSerivce.Controllers
{
    public class SocialCommunityController : ApiController
    {
        ICommunication _com;
        
        public SocialCommunityController(ICommunication com, IRepository repos)
        {
            _com = com;
            
        }

        [HttpPost]
        [Route("GetFollowers")]
        public IHttpActionResult GetFollowers([FromBody]string blockerId)
        {

            var followers = _com.GetLinkers<Profile, Profile>(blockerId, Linkage.Block);

            return Ok(followers);


        }

        [HttpPost]
        [Route("GetFollowed")]
        public IHttpActionResult GetFollowed([FromBody]string blockerId)
        {

            var followers = _com.GetLinkedBy<Profile, Profile>(blockerId, Linkage.Block);

            return Ok(followers);

        }

        [HttpPost]
        [Route("GetPostsByFollowed")]
        public IHttpActionResult GetPostsByFollowed([FromBody]string blockerId)
        {

            var followers = _com.GetLinkedByLinkedBy<Profile,Profile,Post>(blockerId, Linkage.Follow,Linkage.Publish);

            return Ok(followers);


        }

    }
}