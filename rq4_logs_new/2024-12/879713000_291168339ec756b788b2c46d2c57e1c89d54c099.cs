using DataAccessLayer.DTO;

namespace Infrastructure.DataAccessLayer
{
    public interface IChatStorage
    {
        ChatDTO CreateChat(ChatDTO chatDto, UserDTO userCreatedBy);
        void CreateMessage(int chatId, MessageDTO message);
        ChatDTO GetChat(int chatId, int userId, bool updateLastReadMessage = false);
        List<ChatDTO> GetChatsUserIsIn(int userId);
        bool IsUserAdmin(int chatId, int userId);
        bool MakeUserAdmin(int chatId, int userId);
        bool RemoveUserFromChat(int chatId, int userId);
        List<MessageDTO> UpdateLastReadMessage(int chatId, int userId);
    }
}