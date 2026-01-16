using AutoMapper;
using Chat_microservice.model;
using Chat_microservice.Nats.Messages;

namespace Chat_microservice.AutoMapperProfiles
{
    public class NatsProfile : Profile
    {
        public NatsProfile()
        {
            CreateMap<BlockCommand, BlockReply>();
            CreateMap<UnblockCommand, UnblockReply>();
        }
    }
}