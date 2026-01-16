using System;
using System.Collections.Generic;
using Machine.Specifications;
using Rhino.Mocks;
using Rhino.Mspec.Contrib;
using zavit.Domain.Messaging.Messages;
using zavit.Web.Mvc.SignalR.Messaging.Broadcasting.DtoFactories;
using zavit.Web.Mvc.SignalR.Messaging.Broadcasting.Dtos;
using zavit.Web.Mvc.SignalR.Messaging.Broadcasting.GroupIds;

namespace zavit.Web.Mvc.Tests.SignalR.Messaging.Boradcasting.DtoFactorie 
{
    [Subject("ReadMessagesBroadcastDtoFactory")]
    public class ReadMessagesBroadcastDtoFactoryTests : TestOf<ReadMessagesBroadcastDtoFactory>
    {
        class When_creating_read_messages_broadcast_request
        {
            Because of = () => _result = Subject.CreateItem(_completelyReadMessages, MessageThreadId, SenderId);

            It should_set_the_group_ids_to_only_contain_id_combination_of_user_and_thread_id = () => _result.GroupIds.ShouldContainOnly(GroupId);

            It should_set_the_connection_ids_to_exclude_to_be_empty =
                () => _result.ConnectionIdsToExclude.ShouldBeEmpty();

            It should_set_the_dto_to_read_messages_dto = () => _result.Dto.ShouldEqual(_readMessagesDto);

            Establish context = () =>
            {
                _completelyReadMessages = new List<Message> { NewInstanceOf<Message>() };

                Injected<IThreadGroupIdProvider>().Stub(p => p.Provide(MessageThreadId, SenderId)).Return(GroupId);

                _readMessagesDto = NewInstanceOf<ReadMessagesDto>();
                Injected<IReadMessagesDtoFactory>()
                    .Stub(f => f.CreateItem(_completelyReadMessages))
                    .Return(_readMessagesDto);
            };

            const int MessageThreadId = 123;
            static IList<Message> _completelyReadMessages;
            static BroadcastRequest<ReadMessagesDto> _result;
            static ReadMessagesDto _readMessagesDto;
            const string GroupId = "test Group ID";
            const int SenderId = 10;
        }
    }
}
