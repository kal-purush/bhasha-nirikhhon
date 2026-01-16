using MPI_work.Entities;

namespace MPI_work
{
    public class DeletedData
    {
        // Удаленный список магазинов
        public List<Shop> DeletedShopList { get; set; } = new List<Shop> { };
        // Удаленный список пользователей
        public List<User> DeletedUserList { get; set; } = new List<User> { };
        // Удаленый список отзывов
        public List<Feedback> DeletedFeedbackList { get; set; } = new List<Feedback> { };
    }
}