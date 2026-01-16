using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Enqueuer.Data.Configuration;
using Enqueuer.Data.DataSerialization;
using Enqueuer.Messages.MessageHandlers;
using Enqueuer.Persistence.Models;
using Enqueuer.Persistence.Repositories;
using Enqueuer.Services;
using Enqueuer.Services.Interfaces;
using Moq;
using NUnit.Framework;
using Telegram.Bot;
using Telegram.Bot.Requests;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using Chat = Telegram.Bot.Types.Chat;
using Queue = Enqueuer.Persistence.Models.Queue;
using User = Telegram.Bot.Types.User;

namespace Enqueuer.Messages.Tests.MessageHandlersTests
{
    [TestFixture]
    public class CreateQueueMessageHandlerTests
    {
        private Mock<IChatService> chatServiceMock;
        private Mock<IUserService> userServiceMock;
        private Mock<IQueueService> queueServiceMock;
        private Mock<IRepository<Queue>> queueRepositoryMock;
        private Mock<IBotConfiguration> botConfigurationMock;
        private Mock<IDataSerializer> dataSerializerMock;
        private CreateQueueMessageHandler messageHandler;
        private Mock<ITelegramBotClient> botClientMock;

        [SetUp]
        public void SetUp()
        {
            this.chatServiceMock = new Mock<IChatService>();
            this.userServiceMock = new Mock<IUserService>();
            this.queueServiceMock = new Mock<IQueueService>();
            this.queueRepositoryMock = new Mock<IRepository<Queue>>();
            this.botConfigurationMock = new Mock<IBotConfiguration>();
            this.dataSerializerMock = new Mock<IDataSerializer>();
            this.messageHandler = new CreateQueueMessageHandler(
                this.chatServiceMock.Object,
                this.userServiceMock.Object,
                this.queueServiceMock.Object,
                this.queueRepositoryMock.Object,
                this.botConfigurationMock.Object,
                this.dataSerializerMock.Object);
            this.botClientMock = new Mock<ITelegramBotClient>();
        }

        [Test]
        public async Task CreateQueueMessageHandlerTests_HandleMessageAsync_CommandHasNoQueueName_SendsMessageWithSuggestionToAddQueueName()
        {
            // Arrange
            var message = new Message() { Text = this.messageHandler.Command, Chat = new Chat() { Type = ChatType.Group } };
            this.botClientMock.Setup(client => client.MakeRequestAsync(
                    It.Is<SendMessageRequest>(request => request.Text.Equals(CreateQueueMessageHandler.PassQueueNameMessage)), default))
                .Verifiable();

            // Act
            await this.messageHandler.HandleMessageAsync(this.botClientMock.Object, message);

            // Assert
            this.botClientMock.Verify();
        }

        [Test]
        public async Task CreateQueueMessageHandlerTests_HandleMessageAsync_ChatHasTheMaximumNumberOfQueues_SendsMessageWithSuggestionToDeleteExistingQueue()
        {
            // Arrange
            var queueName = "Test";
            var chatId = 1L;
            var chat = new Chat() {Id = chatId, Type = ChatType.Group};
            var maxNumberOfQueues = 5;
            var message = new Message() { Text = string.Join(' ', this.messageHandler.Command, queueName), Chat = chat };
            
            this.botConfigurationMock.Setup(configuration => configuration.QueuesPerChat)
                .Returns(maxNumberOfQueues);

            this.chatServiceMock.Setup(chatService => chatService.GetNumberOfQueues(chatId))
                .Returns(maxNumberOfQueues);
            this.chatServiceMock.Setup(chatService => chatService.GetNewOrExistingChatAsync(chat))
                .Returns(Task.FromResult(new Persistence.Models.Chat() { ChatId = chatId }));

            this.userServiceMock.Setup(userService => userService.GetNewOrExistingUserAsync(It.IsAny<User>()))
                .Returns(Task.FromResult(It.IsAny<Persistence.Models.User>()));

            this.botClientMock.Setup(client => client.MakeRequestAsync(
                    It.Is<SendMessageRequest>(request => request.Text.Equals(CreateQueueMessageHandler.ChatReachedMaximumQueuesMessage)), default))
                .Verifiable();

            // Act
            await this.messageHandler.HandleMessageAsync(this.botClientMock.Object, message);

            // Assert
            this.botClientMock.Verify();
        }
    }
}