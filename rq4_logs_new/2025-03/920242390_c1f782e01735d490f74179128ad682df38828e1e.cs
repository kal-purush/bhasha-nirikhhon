using API.DTO;
using API.Entities;
using API.Extensions;
using API.Helpers;
using API.Interfaces;
using AutoMapper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers;
[Authorize]
public class MessagesController(IMessageRepository messageRepo, IUserRepository userRepo,
    IMapper mapper) : BaseApiController
{
    [HttpPost]
    public async Task<ActionResult<MessageDto>> CreateMessageAsync(CreateMessageDto createMessageDto)
    {
        var username = User.GetUsername();
        if (username == createMessageDto.RecipientUsername.ToLower())
        {
            return BadRequest("You cannot send messages to yourself");
        }
        var sender = await userRepo.GetUserByUsernameAsync(username);
        var recipient = await userRepo.GetUserByUsernameAsync(createMessageDto.RecipientUsername);
        if (recipient is null || sender is null)
        {
            return BadRequest("User not found");
        }
        var message = new Message
        {
            Sender = sender,
            Recipient = recipient,
            SenderUsername = sender.UserName,
            RecipientUsername = recipient.UserName,
            Content = createMessageDto.Content
        };
        messageRepo.AddMessage(message);
        if (await messageRepo.SaveAllChangesAsync())
        {
            return Ok(mapper.Map<MessageDto>(message));
        }
        return BadRequest("Failed to send message");
    }

    [HttpGet]
    public async Task<ActionResult<IEnumerable<MessageDto>>> GetMessagesForUserAsync(
        [FromQuery]MessageParams messageParams)
    {
        messageParams.Username = User.GetUsername();
        var messages = await messageRepo.GetMessagesForUserAsync(messageParams);
        Response.AddPaginationHeader(messages);
        return messages;
    }

    [HttpGet("thread/{username}")]
    public async Task<ActionResult<IEnumerable<MessageDto>>> GetMessagesThreadAsync(string username)
    {
        var currentUserUsername = User.GetUsername();
        
        return Ok(await messageRepo.GetMessagesThreadAsync(currentUserUsername, username));
    }

    [HttpDelete("{id}")]
    public async Task<ActionResult> DeleteMessageAsync(int id)
    {
        var currentUserUsername = User.GetUsername();
        var message = await messageRepo.GetMessageAsync(id);
        if (message is null)
        {
            return BadRequest("Message not found");
        }
        if (message.SenderUsername != currentUserUsername && 
            message.RecipientUsername != currentUserUsername)
        {
            return Forbid();
        }
        if (message.SenderUsername == currentUserUsername)
        {
            message.SenderDeleted = true;
        }
        
        if(message.RecipientUsername == currentUserUsername)
        {
            message.RecipientDeleted = true;
        }

        if (message is {SenderDeleted:true, RecipientDeleted:true})
        {
            messageRepo.DeleteMessage(message);
        }
        
        if (await messageRepo.SaveAllChangesAsync())
        {
            return Ok();
        }
        
        return BadRequest("Problem deleting the message");
    }
}