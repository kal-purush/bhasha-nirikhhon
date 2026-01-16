using core.Helpers;
using Core.Models.DTO;
using DataAccessLayer.DTO;
using UnitTests.MockDataAccess;

namespace UnitTests
{
    [TestClass]
    public class ChatServiceTests
    {
        [TestMethod]
        public void ShouldReturnCorrectChat_WhenGetChatIsCalled()
        {
            // Arrange
            var chatStorage = new MockChatStorage();
            var userStorage = new MockUserStorage();

            var chatService = new ChatService(chatStorage);

            userStorage.CreateUser(new UserDTO("Ryan"), "fontys123");
            userStorage.CreateUser(new UserDTO("Peter"), "fontys123");

            chatStorage.CreateChat(new ChatDTO("Coole groepschat", new() {
                new("Ryan", 1, true),
                new("Peter", 2, true)
            }), userStorage.GetUserById(1));


            chatStorage.CreateChat(new ChatDTO("Niet coole groepschat", new() {
                new("Ryan", 1, true),
                new("Peter", 2, true)
            }), userStorage.GetUserById(2));

            // Act

            var chat1 = chatService.GetChat(1, 1, false);
            var chat2 = chatService.GetChat(2, 1, false);

            // Assert

            Assert.AreEqual("Coole groepschat", chat1.Name);
            Assert.AreEqual("Niet coole groepschat", chat2.Name);
        }

        public void ShouldReturnAllChatsUserIsIn_WhenGetAllChatsUserIsInIsCalled()
        {
            // Arrange
            var chatStorage = new MockChatStorage();
            var userStorage = new MockUserStorage();

            var chatService = new ChatService(chatStorage);

            userStorage.CreateUser(new UserDTO("Ryan"), "fontys123");
            userStorage.CreateUser(new UserDTO("Peter"), "fontys123");
            userStorage.CreateUser(new UserDTO("Jan"), "fontys123");
            userStorage.CreateUser(new UserDTO("Bob"), "fontys123");

            chatStorage.CreateChat(new ChatDTO("Coole groepschat", new() {
                new("Ryan", 1, true),
                new("Peter", 2, true),
                new("Jan", 3, true)
            }), userStorage.GetUserById(1));


            chatStorage.CreateChat(new ChatDTO("Niet coole groepschat", new() {
                new("Ryan", 1, true),
                new("Bob", 4, true),
            }), userStorage.GetUserById(4));

            chatStorage.CreateChat(new ChatDTO("Niet coole groepschat", new() {
                new("Bob", 4, true),
                new("Jan", 3, true)
            }), userStorage.GetUserById(3));

            chatStorage.CreateChat(new ChatDTO("Niet coole groepschat", new() {
                new("Ryan", 1, true),
                new("Jan", 3, true)
            }), userStorage.GetUserById(1));

            chatStorage.CreateChat(new ChatDTO("Niet coole groepschat", new() {
                new("Bob", 4, true),
                new("Peter", 2, true),
                new("Jan", 3, true)
            }), userStorage.GetUserById(4));

            // act
            var chatsRyan = chatService.GetAllChatsUserIsIn(1);
            var chatsPeter = chatService.GetAllChatsUserIsIn(2);
            var chatsJan = chatService.GetAllChatsUserIsIn(3);

            //Test
            Assert.Equals(3, chatsRyan.Count);
            Assert.Equals(2, chatsPeter.Count);
            Assert.Equals(4, chatsJan.Count);
        }

        [TestMethod]
        public void ShouldAllowOnlyAdminsToCreateAdmins_WhenAttemptingToMakeUserAdmin()
        {
            // Arrange
            var chatStorage = new MockChatStorage();
            var userStorage = new MockUserStorage();

            var chatService = new ChatService(chatStorage);

            userStorage.CreateUser(new UserDTO("Ryan"), "fontys123");
            userStorage.CreateUser(new UserDTO("Peter"), "fontys123");

            var ryan = userStorage.GetUserById(1);
            var peter = userStorage.GetUserById(2);

            chatStorage.CreateChat(new ChatDTO("Coole groepschat", new() {
                new("Ryan", 1, false),
                new("Peter", 2, true)
            }), userStorage.GetUserById(2));


            chatStorage.CreateChat(new ChatDTO("Niet coole groepschat", new() {
                new("Ryan", 1, true),
                new("Peter", 2, false)
            }), userStorage.GetUserById(1));

            var chat1 = chatService.GetGroupChat(1, 1);
            var chat2 = chatService.GetGroupChat(2, 1);

            // Act

            bool ryanCanCreateAdminsInChat1 = chat1.MakeUserAdmin(chatStorage, peter, ryan.Id.Value); 
            bool ryanCanCreateAdminsInChat2 = chat2.MakeUserAdmin(chatStorage, peter, ryan.Id.Value);

            bool peterCanCreateAdminsInChat1 = chat1.MakeUserAdmin(chatStorage, ryan, peter.Id.Value);
            bool peterCanCreateAdminsInChat2 = chat2.MakeUserAdmin(chatStorage, ryan, peter.Id.Value);

            // Assert

            Assert.IsTrue(!ryanCanCreateAdminsInChat1);
            Assert.IsTrue(ryanCanCreateAdminsInChat2);
            Assert.IsTrue(peterCanCreateAdminsInChat1);
            Assert.IsTrue(peterCanCreateAdminsInChat2); // peter should be admin now
        }


        [TestMethod]
        public void ShouldReturnCorrectOffsetAndUpdateLastReadMessage_WhenFetchChatUpdatesIsCalled()
        {
            // Arrange
            var chatStorage = new MockChatStorage();
            var userStorage = new MockUserStorage();

            var chatService = new ChatService(chatStorage);

            userStorage.CreateUser(new UserDTO("Ryan"), "fontys123");
            userStorage.CreateUser(new UserDTO("Peter") { FirstName = "Peter", LastName = "Denbos" }, "fontys123");
            userStorage.CreateUser(new UserDTO("Bob"), "fontys123");

            var ryan = userStorage.GetUserById(1);
            var peter = userStorage.GetUserById(2);
            var bob = userStorage.GetUserById(3);

            chatStorage.CreateChat(new ChatDTO("Coole groepschat", new() {
                new("Ryan", 1, false),
                new("Peter", 2, true){ FirstName = "Peter", LastName = "Denbos"},
                new("Bob", 3, false)
            }), userStorage.GetUserById(2));

            chatStorage.CreateChat(new ChatDTO("Super Coole groepschat", new() {
                new("Ryan", 1, false),
                new("Peter", 2, true){ FirstName = "Peter", LastName = "Denbos"},
                new("Bob", 3, false)
            }), userStorage.GetUserById(2));


            var chat = chatService.GetGroupChat(1, peter.Id.Value);
            var chat2 = chatService.GetGroupChat(2, peter.Id.Value);
            
            // Act
            
            chat2.RemoveUserFromChat(chatStorage, ryan, peter); // remove ryan from chat

            // Watch out, creating a new chat also creates an unread system message

            chatStorage.CreateMessage(1, new("Hi Ryan!", peter));
            chatStorage.CreateMessage(1, new("Hoe gaat het?", peter));

            var chatUpdates1 = chatService.FetchChatUpdates(ryan.Id.Value, new List<ChatPollDTO>()
            {
                new(){ Hash = chat.Hash, IsOpen = true, LastFetchedMessageId = 1, Id = 1},
                new(){ Hash = chat2.Hash, IsOpen = false, LastFetchedMessageId = 1, Id = 2}
            });

            var chatUpdates2 = chatService.FetchChatUpdates(ryan.Id.Value, new List<ChatPollDTO>()
            {
                new(){ Hash = chat.Hash, IsOpen = true, LastFetchedMessageId = 1, Id = 1}
            });

            // Assert

            Assert.AreEqual(2, chatUpdates1[0].messages.Count);
            Assert.IsTrue(chatUpdates1[1].Removed);

            Assert.AreEqual("Hi Ryan!", chatUpdates1[0].messages[0].Content);
            Assert.AreEqual("Hoe gaat het?", chatUpdates1[0].messages[1].Content);

            // no updates messages are now set as read
            Assert.AreEqual(chatUpdates2.Count, 0);
        }
    }
}