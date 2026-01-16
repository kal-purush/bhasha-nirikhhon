using Microsoft.AspNetCore.Mvc;
using System;
using System.Linq;
using System.Threading.Tasks;
using TheReplacement.PTA.Common.Enums;
using TheReplacement.PTA.Common.Models;
using TheReplacement.PTA.Common.Utilities;
using TheReplacement.PTA.Services.Core.Extensions;
using TheReplacement.PTA.Services.Core.Messages;

namespace TheReplacement.PTA.Services.Core.Controllers
{
    [ApiController]
    [Route("api/v1/user")]
    public class UserController : PtaControllerBase
    {
        protected override MongoCollection Collection => throw new NotImplementedException();

        [HttpGet("{userId}/{messageId}")]
        public ActionResult<UserMessageThreadModel> GetMessage(string userId, string messageId)
        {
            if (!Request.VerifyIdentity(userId))
            {
                return Unauthorized();
            }

            var user = DatabaseUtility.FindUserById(userId);
            if (!user.Messages.Contains(messageId))
            {
                return Conflict();
            }

            return DatabaseUtility.FindMessageById(messageId);
        }

        [HttpPost]
        public ActionResult<FoundUserMessage> CreateNewUser([FromQuery] string username, [FromQuery] string password)
        {
            if (username.Length < 6 || password.Length < 6)
            {
                return BadRequest();
            }

            if (DatabaseUtility.FindUserByUsername(username) != null)
            {
                return Conflict(username);
            }

            var user = new UserModel(username, password);
            if (DatabaseUtility.TryAddUser(user, out var error))
            {
                Response.AssignAuthAndToken(user.UserId);
                return new FoundUserMessage(user);
            }

            return BadRequest(error);
        }

        [HttpPost("{userId}/{recipientId}/sendMessage")]
        public async Task<ActionResult> SendMessageAsync(string userId, string recipientId)
        {
            if (!Request.VerifyIdentity(userId))
            {
                return Unauthorized();
            }

            var user = DatabaseUtility.FindUserById(userId);
            var recipient = DatabaseUtility.FindUserById(recipientId);

            if (recipient == null)
            {
                return NotFound(recipientId);
            }

            var body = await Request.GetRequestBody();
            var messageContent = body["messageContent"]?.ToString();
            if (string.IsNullOrEmpty(messageContent))
            {
                return BadRequest();
            }

            var error = AddNewThreadToUsers(user, recipient, messageContent);
            if (error != null)
            {
                return BadRequest(error);
            }

            return Ok();
        }

        [HttpPut("{userId}/{messageId}/replyMessage")]
        public async Task<ActionResult> ReplyMessageAsync(string userId, string messageId)
        {
            if (!Request.VerifyIdentity(userId))
            {
                return Unauthorized();
            }

            var user = DatabaseUtility.FindUserById(userId);
            var thread = DatabaseUtility.FindMessageById(messageId);
            if (!user.Messages.Contains(messageId))
            {
                return Conflict();
            }

            var body = await Request.GetRequestBody();
            var messageContent = body["messageContent"]?.ToString();
            if (string.IsNullOrEmpty(messageContent))
            {
                return BadRequest();
            }

            AddNewReplyToThread(user, thread, messageContent);
            return Ok();
        }

        [HttpPut("{gameId}/{userId}/refresh")]
        public ActionResult RefreshInGame(string userId, string gameId, [FromQuery] bool isGM)
        {
            if (isGM)
            {
                return GetUpdatedGM(userId, gameId);
            }

            return GetUpdatedTrainer(userId, gameId);
        }

        [HttpPut("login")]
        public async Task<ActionResult<FoundUserMessage>> Login()
        {
            var (username, password, credentialErrors) = await Request.GetUserCredentials();
            if (credentialErrors.Any())
            {
                return BadRequest(credentialErrors);
            }

            if (!IsUserAuthenticated(username, password, out var authError))
            {
                return authError;
            }

            var user = DatabaseUtility.FindUserByUsername(username);
            Response.AssignAuthAndToken(user.UserId);
            return new FoundUserMessage(user);
        }

        [HttpPut("{userId}/logout")]
        public ActionResult Logout(string userId)
        {
            if (!Request.VerifyIdentity(userId))
            {
                return Unauthorized();
            }

            DatabaseUtility.UpdateUserOnlineStatus(userId, false);
            return Ok();
        }

        private ActionResult GetUpdatedTrainer(string userId, string gameId)
        {
            if (!Request.VerifyIdentity(userId))
            {
                return Unauthorized();
            }

            Response.RefreshToken(userId);
            return Ok(new FoundTrainerMessage(userId, gameId));
        }

        private ActionResult GetUpdatedGM(string userId, string gameId)
        {
            if (!Request.IsUserGM(userId, gameId))
            {
                return Unauthorized();
            }

            Response.RefreshToken(userId);
            return Ok(new GameMasterMessage(userId, gameId));
        }

        private static MongoWriteError AddNewThreadToUsers(UserModel sender, UserModel recipient, string messageContent)
        {
            var message = new UserMessageModel(sender.UserId, messageContent);
            var thread = new UserMessageThreadModel
            {
                MessageId = Guid.NewGuid().ToString(),
                Messages = new[] { message }
            };

            if (DatabaseUtility.TryAddThread(thread, out var error))
            {
                sender.Messages = sender.Messages.Append(thread.MessageId);
                recipient.Messages = recipient.Messages.Append(thread.MessageId);
                DatabaseUtility.UpdateUser(sender);
                DatabaseUtility.UpdateUser(recipient);
            }

            return error;
        }

        private static void AddNewReplyToThread(UserModel sender, UserMessageThreadModel thread, string messageContent)
        {
            var message = new UserMessageModel(sender.UserId, messageContent);
            thread.Messages = thread.Messages.Append(message);
            DatabaseUtility.UpdateThread(thread);
        }
    }
}