using System.Threading.Tasks;
using Enqueuer.Messages.MessageHandlers;
using Enqueuer.Persistence.Models;
using Enqueuer.Persistence.Repositories;
using Enqueuer.Services.Interfaces;
using Moq;
using NUnit.Framework;
using Telegram.Bot;
using Telegram.Bot.Requests;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using Chat = Telegram.Bot.Types.Chat;
using User = Telegram.Bot.Types.User;

namespace Enqueuer.Messages.Tests.MessageHandlersTests
{
    [TestFixture]
    public class EnqueueMessageHandlerTests
    {
        private Mock<IChatService> chatServiceMock;
        private Mock<IUserService> userServiceMock;
        private Mock<IQueueService> queueServiceMock;
        private Mock<IUserInQueueService> userInQueueServiceMock;
        private Mock<IRepository<UserInQueue>> userInQueueRepositoryMock;
        private EnqueueMessageHandler messageHandler;
        private Mock<ITelegramBotClient> botClientMock;
        public const char Whitespace = ' ';

        [SetUp]
        public void SetUp()
        {
            this.chatServiceMock = new Mock<IChatService>();
            this.userServiceMock = new Mock<IUserService>();
            this.queueServiceMock = new Mock<IQueueService>();
            this.userInQueueServiceMock = new Mock<IUserInQueueService>();
            this.userInQueueRepositoryMock = new Mock<IRepository<UserInQueue>>();
            this.messageHandler = new EnqueueMessageHandler(
                this.chatServiceMock.Object,
                this.userServiceMock.Object,
                this.queueServiceMock.Object,
                this.userInQueueServiceMock.Object,
                this.userInQueueRepositoryMock.Object);
            this.botClientMock = new Mock<ITelegramBotClient>();
        }

        [Test]
        public async Task EnqueueMessageHandlerTests_HandleMessageAsync_CommandHasNoQueueName_SendsMessageWithSuggestionToAddQueueName()
        {
            // Arrange
            var message = new Message()
            {
                Text = this.messageHandler.Command,
                Chat = new Chat() { Type = ChatType.Group }
            };

            this.botClientMock.Setup(client => client.MakeRequestAsync(
                    It.Is<SendMessageRequest>(
                        request => request.Text.Equals(EnqueueMessageHandler.PassQueueNameMessage)), default))
                .Verifiable();

            // Act
            await this.messageHandler.HandleMessageAsync(this.botClientMock.Object, message);

            // Assert
            this.botClientMock.Verify();
        }

        [TestCase(-1)]
        [TestCase(0)]
        public async Task EnqueueMessageHandlerTests_HandleMessageAsync_PositionIsInvalid_DoesNotAddUserToQueue(int invalidPosition)
        {
            // Arrange
            const string queueName = "Test";
            const int chatId = 1;
            var telegramChat = new Chat() { Type = ChatType.Group};
            var message = new Message()
            {
                Text = string.Join(Whitespace, this.messageHandler.Command, queueName, invalidPosition),
                Chat = telegramChat
            };

            this.chatServiceMock.Setup(chatService => chatService.GetNewOrExistingChatAsync(telegramChat))
                .Returns(Task.FromResult(new Persistence.Models.Chat() { ChatId = chatId}));

            this.userServiceMock.Setup(userService => userService.GetNewOrExistingUserAsync(It.IsAny<User>()))
                .Returns(Task.FromResult(It.IsAny<Persistence.Models.User>()));

            this.userInQueueServiceMock.Setup(userInQueueService => userInQueueService.AddUserToQueue(It.IsAny<Persistence.Models.User>(), It.IsAny<Queue>(), invalidPosition))
                .Verifiable();

            this.botClientMock.Setup(client => client.MakeRequestAsync(It.IsAny<SendMessageRequest>(), default)).Verifiable();

            // Act
            await this.messageHandler.HandleMessageAsync(this.botClientMock.Object, message);

            // Assert
            this.userInQueueServiceMock.Verify(userInQueueService => userInQueueService.AddUserToQueue(It.IsAny<Persistence.Models.User>(), It.IsAny<Queue>(), It.IsAny<int>()), Times.Never);
        }

        [Test]
        public async Task EnqueueMessageHandlerTests_HandleMessageAsync_PositionIsNegative_SendsMessageWithDescription()
        {
            // Arrange
            const string queueName = "Test";
            const int negativePosition = -1;
            const int chatId = 1;
            var telegramChat = new Chat() { Type = ChatType.Group };
            var message = new Message()
            {
                Text = string.Join(Whitespace, this.messageHandler.Command, queueName, negativePosition),
                Chat = telegramChat
            };

            this.chatServiceMock.Setup(chatService => chatService.GetNewOrExistingChatAsync(telegramChat))
                .Returns(Task.FromResult(new Persistence.Models.Chat() { ChatId = chatId }));

            this.userServiceMock.Setup(userService => userService.GetNewOrExistingUserAsync(It.IsAny<User>()))
                .Returns(Task.FromResult(It.IsAny<Persistence.Models.User>()));

            this.botClientMock.Setup(client => client.MakeRequestAsync(
                It.Is<SendMessageRequest>(
                    request => request.Text.Equals(EnqueueMessageHandler.InvalidQueuePositionMessage)), default))
                .Verifiable();

            // Act
            await this.messageHandler.HandleMessageAsync(this.botClientMock.Object, message);

            // Assert
            this.botClientMock.Verify();
        }
    }
}