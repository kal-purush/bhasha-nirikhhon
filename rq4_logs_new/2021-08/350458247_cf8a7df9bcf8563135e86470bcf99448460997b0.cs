using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using pp_bot.Server.Helpers;
using pp_bot.Server.Models;
using Telegram.Bot;
using Telegram.Bot.Types;

namespace pp_bot.Server.Achievements
{
    public class EuropeAchievement : IAchievable
    {
        private const int ACHIEVEMENT_ID  = 3;
        private readonly PP_Context _context;
        private readonly DatabaseHelper _databaseHelper;
        private readonly ITelegramBotClient _client;
        
        public EuropeAchievement(ITelegramBotClient client, PP_Context context)
        {
            _client = client;
            _context = context;
            _databaseHelper = new DatabaseHelper(context);
        }

        public async Task IsAcquiredAsync(Message m, CancellationToken ct)
        {
            var achievement = await _context.Achievements.FirstOrDefaultAsync(a => a.Id == ACHIEVEMENT_ID, ct);
            var userChat = await _databaseHelper.GetUserChatAsync(m,ct);

            if(achievement == null)
                throw new NotImplementedException($"Achievement with id {ACHIEVEMENT_ID} was not found");

            if(userChat.AcquiredAchievements.Contains(achievement))
                return;

            if(userChat.PPLength >= 14)
            {
                userChat.AcquiredAchievements.Add(achievement);
                achievement.UsersAcquired.Add(userChat);

                _context.Update(userChat);
                _context.Update(achievement);
                await _context.SaveChangesAsync(ct);

                await _client.SendTextMessageAsync
                (
                    m.Chat.Id,
                    $"<b>{userChat.User.Username}</b> получил достижение <i>{achievement.Name}</i>, поздравляем 🎉!",
                    parseMode: Telegram.Bot.Types.Enums.ParseMode.Html,
                    cancellationToken: ct
                );
            }
        }
    }
}