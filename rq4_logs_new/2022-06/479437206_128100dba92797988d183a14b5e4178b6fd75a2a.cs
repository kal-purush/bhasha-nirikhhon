using System;
using System.Collections;
using System.Collections.Generic;
using AutoMapper;
using Chat_microservice.model;
using Chat_microservice.Protos;
using Chat_microservice.Utilities;

namespace Chat_microservice.AutoMapperProfiles
{
    public class ChatProfile : Profile
    {
        public ChatProfile()
        {
            CreateMap<ChatMessage, Message>()
                .ForMember(m => m.SenderId, opt => opt.MapFrom(src => src.UserId.ToString()))
                .ForMember(m => m.TimeSent, opt => opt.MapFrom(src => TimeUtility.GetUnixTimeStamp(src.CreatedDate)));
            CreateMap<Chat, ChatMsg>()
                .ForMember(c => c.FirstParticipantId, opt => opt.MapFrom(src => src.FirstParticipant.UserId.ToString()))
                .ForMember(c => c.SecondParticipantId,
                    opt => opt.MapFrom(src => src.SecondParticipant.UserId.ToString()));
            CreateMap<IEnumerable<Chat>, ChatsMsg>()
                .ForMember(c => c.Chats, opt => opt.MapFrom(src => src));
            CreateMap<NewMessage, Chat>()
                .ForPath(c => c.FirstParticipant.UserId, opt => opt.MapFrom(src => Guid.Parse(src.SenderId)))
                .ForPath(c => c.SecondParticipant.UserId, opt => opt.MapFrom(src => Guid.Parse(src.ReceiverId)))
                .ForPath(c => c.Messages,
                    opt => opt.MapFrom(src => new List<ChatMessage>
                    {
                        new() { CreatedDate = DateTime.UtcNow, Text = src.Text, UserId = Guid.Parse(src.SenderId) }
                    }));
            CreateMap<NewMessage, ChatMessage>()
                .ForMember(c => c.UserId, opt => opt.MapFrom(src => Guid.Parse(src.SenderId)))
                .AfterMap((m, c) => c.CreatedDate = DateTime.UtcNow);
        }
    }
}