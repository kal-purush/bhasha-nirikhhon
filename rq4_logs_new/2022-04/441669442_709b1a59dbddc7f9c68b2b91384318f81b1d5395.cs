using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using API.DTOs;
using API.Entities;
using API.Extensions;
using API.Helpers;
using API.interfaces;
using AutoMapper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers
{
    [Authorize]
    public class MessagesController : BaseApiController
    {
        private readonly IUserRepository _userRepository;
        private readonly IMessageRepository _messageRepository;
        private readonly IMapper _mapper;

        public MessagesController(IUserRepository userRepository, IMessageRepository messageRepository, IMapper mapper)
        {
            this._userRepository = userRepository;
            this._messageRepository = messageRepository;
            this._mapper = mapper;
        }

        [HttpPost]
        public async Task<ActionResult<MessageDto>> CreateMessage(CreateMessageDto createMessageDto){
            var username = User.GetUsername();

            if(username == createMessageDto.RecipientUsername.ToLower()){
                return BadRequest("You cannot send a message to yourself");
            }

            var sender = await _userRepository.GetUserByUserNameAsync(username);
            var recipient = await _userRepository.GetUserByUserNameAsync(createMessageDto.RecipientUsername);

            if(sender == null || recipient == null){
                return BadRequest("Could not find user");
            }

           
            var message = new Message{
                Sender = sender,
                Recipient = recipient,
                Content = createMessageDto.Content,
                SenderUsername = sender.UserName,
                RecipientUsername = recipient.UserName
            };

            _messageRepository.AddMessage(message);

            if(await _messageRepository.SaveAllAsync()){
                return Ok(_mapper.Map<MessageDto>(message));
            }

            return BadRequest("Failed to send message");
        }


        [HttpGet]
        public async Task<ActionResult<IEnumerable<MessageDto>>> GetMessagesForUser([FromQuery]MessageParams messageParams){
           messageParams.Username = User.GetUsername();

           var messages = await _messageRepository.GetMessagesForUser(messageParams);

           Response.AddPaginationHeader(messages.CurrentPage, messages.PageSize, messages.TotalCount, messages.TotalPages);

           return Ok(messages);
        }


        [HttpGet("thread/{username}")]
        public async Task<ActionResult<IEnumerable<MessageDto>>> GetMessageThread(string username){
            var currentUsername = User.GetUsername();
            var messages = await _messageRepository.GetMessageThread(currentUsername, username);
            return Ok(messages);
        }


        [HttpDelete("{id}")]
        public async Task<ActionResult> DeleteMessage(int id){
            var username = User.GetUsername();
            var message = await _messageRepository.GetMessage(id);

            if(message == null){
                return NotFound();
            }

            if(message.SenderUsername == username){
                message.SenderDeleted = true;
            }
            else if(message.RecipientUsername == username){
                message.RecipientDeleted = true;
            }
            else{
                return Unauthorized();
            }
            if(message.SenderDeleted && message.RecipientDeleted){
                _messageRepository.DeleteMessage(message);
            }

            if(await _messageRepository.SaveAllAsync()){
                return NoContent();
            }

            return BadRequest("Failed to delete message");
        }
    }
}