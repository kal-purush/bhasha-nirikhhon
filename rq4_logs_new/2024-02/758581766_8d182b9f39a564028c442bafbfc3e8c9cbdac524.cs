using user_monitoring.Models;

namespace user_monitoring.Services.Interfaces
{
    public interface IWordBanListService
    {
        public bool Save(WordBanList wordBanList);

        public bool Load();
    }
}