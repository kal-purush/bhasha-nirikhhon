using Machine.Specifications;
using Rhino.Mocks;
using Rhino.Mspec.Contrib;
using zavit.Domain.Accounts;
using zavit.Domain.Messaging;
using zavit.Domain.Messaging.Messages;
using zavit.Web.Api.DtoFactories.Messaging.Messages;
using zavit.Web.Api.Dtos.Messaging.Messages;
using zavit.Web.Mvc.SignalR.ConnectionIds;
using zavit.Web.Mvc.SignalR.Messaging.Broadcasting.DtoFactories;
using zavit.Web.Mvc.SignalR.Messaging.Broadcasting.Dtos;
using zavit.Web.Mvc.SignalR.Messaging.Broadcasting.GroupIds;

namespace zavit.Web.Mvc.Tests.SignalR.Messaging.Boradcasting.DtoFactorie 
{
    [Subject("InboxMessageBroadcastRequestFactory")]
    public class InboxMessageBroadcastRequestFactoryTests : TestOf<InboxMessageBroadcastRequestFactory>
    {
        class When_creatingg_broadcast_request_dto
        {
            Because of = () => _result = Subject.CreateItem(_message);

            It should_set_the_dto_to_be_the_new_message_dto = () => _result.Dto.ShouldEqual(_messageDto);

            It should_set_group_to_ids_made_from_recipient_ids_including_the_sender =
                () => _result.GroupIds.ShouldContainOnly(GroupId, CurrentUserGroupId);

            It should_set_the_excluded_ids_to_be_empty =
                () => _result.ConnectionIdsToExclude.ShouldBeEmpty();

            Establish context = () =>
            {
                var recipient = NewInstanceOf<Account>();
                recipient.Id = 123;

                _message = NewInstanceOf<Message>();
                _message.Sender = NewInstanceOf<Account>();
                _message.Sender.Id = 6677;
                _message.Stub(m => m.GetRecipients()).Return(new[] { recipient });

                _messageDto = NewInstanceOf<MessageDto>();
                Injected<IMessageDtoFactory>().Stub(f => f.CreateItem(_message)).Return(_messageDto);

                Injected<IInboxGroupIdProvider>()
                    .Stub(p => p.Provide(recipient.Id))
                    .Return(GroupId);

                Injected<IInboxGroupIdProvider>()
                    .Stub(p => p.Provide(_message.Sender.Id))
                    .Return(CurrentUserGroupId);
            };

            static BroadcastRequest<MessageDto> _result;
            static Message _message;
            static MessageDto _messageDto;
            const string GroupId = "groupId";
            const string CurrentUserGroupId = "otherGroupId";
        }
    }
}
