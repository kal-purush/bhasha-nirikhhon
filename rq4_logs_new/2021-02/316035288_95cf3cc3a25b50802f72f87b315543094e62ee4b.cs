using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Chatbot.Core.Common;
using Chatbot.Abstractions.Contracts.Responses;
using Chatbot.Abstractions.Core;
using Chatbot.Abstractions.Core.Services;
using Chatbot.Model.DataModel;
using Chatbot.Model.Enums;
using Newtonsoft.Json;

namespace Chatbot.Core
{
    public class ChatBotHelper: IChatBotHelper
    {
        private readonly IMessageService _messageService;
        private readonly IQuestionService _questionService;
        private readonly Mapper _mapper;

        public ChatBotHelper(
            IMessageService messageService,
            IQuestionService questionService,
            Mapper mapper)
        {
            _messageService = messageService;
            _questionService = questionService;
            _mapper = mapper;
        }

        public async Task<MessageResponse> GetResponse(Message message)
        {
            var msg = await _messageService.Add(new Message()
            {
                Id = Guid.NewGuid(),
                Content = "Здравствуйте! Я робот-помощник АО РЭС. " +
                          "Выберите тему из списка ниже или заполните форму для связи с оператором",
                Owner = MessageOwner.ChatbotHelper,
                Sender = default,
                Status = MessageStatus.Saved,
                Time = DateTime.UtcNow,
                Type = MessageType.String,
                MessageDialogId = message.MessageDialogId
            });
            return _mapper.Map<MessageResponse>(msg);
        }

        public async Task<MessageResponse[]> GetQuestionMessages(Message message)
        {
            var questions = await _questionService.GetQuestions(Guid.Empty);
            var messages = new List<Message>();
            foreach (var question in questions)
            {
                var msg = await _messageService.Add(new Message()
                {
                    Id = Guid.NewGuid(),
                    Content = JsonConvert.SerializeObject(new { question.Id, question.Question }),
                    Owner = MessageOwner.ChatbotHelper,
                    Sender = default,
                    Status = MessageStatus.Received,
                    Time = DateTime.UtcNow,
                    Type = MessageType.Question,
                    MessageDialogId = message.MessageDialogId
                });
                
                messages.Add(msg);
            }

            return _mapper.Map<MessageResponse[]>(messages);
        }

        public async Task<MessageResponse> GetButtonForForm(Message message)
        {
            var msg = await _messageService.Add(new Message()
            {
                Id = Guid.NewGuid(),
                Content = "Заполнить форму",
                Owner = MessageOwner.ChatbotHelper,
                Sender = default,
                Status = MessageStatus.Received,
                Time = DateTime.UtcNow,
                Type = MessageType.ButtonForForm,
                MessageDialogId = message.MessageDialogId
            });

            return _mapper.Map<MessageResponse>(msg);
        }
    }
}