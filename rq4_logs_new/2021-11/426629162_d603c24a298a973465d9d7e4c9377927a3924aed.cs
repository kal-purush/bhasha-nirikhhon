using ExpertDirectory.Models;
using ExpertDirectory.Repositories;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpertDirectory.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class FriendsController : ControllerBase
    {
        private readonly IFriendRepository _friendRepository;

        public FriendsController(IFriendRepository friendRepository)
        {
            _friendRepository = friendRepository;
        }

        /// <summary>
        /// Gets all friends.
        /// </summary>
        [HttpGet]
        public List<FriendsDetails> GetFriends()
        {
            return _friendRepository.Get();
        }

        /// <summary>
        /// Get member's friend.
        /// </summary>
        [HttpGet("{id}")]
        public List<FriendsDetails> GetFriends(int id)
        {
            return _friendRepository.Get(id);
        }

        /// <summary>
        /// Make a member friend of another member.
        /// </summary>
        [HttpPost]
        public async Task<ActionResult<Friends>> PostFriends([FromBody] Friends friend)
        {
            var newFriend = await _friendRepository.Create(friend);

            Friends alterfriend = new Friends();
            alterfriend.MemberId = friend.FriendId;
            alterfriend.FriendId = friend.MemberId;

            newFriend = await _friendRepository.Create(alterfriend);
            return NoContent();
        }
    }
}