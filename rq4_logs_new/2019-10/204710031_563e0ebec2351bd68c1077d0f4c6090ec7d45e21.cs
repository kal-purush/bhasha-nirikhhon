using LPChat.Domain.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace LPChat.Infrastructure.Services
{
    public class ChatManager
    {
        private readonly IChatService _chatService;
        private readonly IMessageService _messageService;

        public ChatManager(IChatService chatService, IMessageService messageService)
        {
            _chatService = chatService;
            _messageService = messageService;
        }

        public void CreateChat()
        {

        }

        public void ChangeChat()
        {

        }


    }
}