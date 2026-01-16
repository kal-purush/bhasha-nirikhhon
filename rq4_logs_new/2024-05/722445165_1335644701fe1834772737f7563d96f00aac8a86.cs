using Data.Context;
using Data.Entities;
using Microsoft.EntityFrameworkCore;
using Telegram.Bot;

namespace Birthdays.TgBot.Services;

public class NotificationCheckerAndSender(
    Bot.Bot bot,
    AppDbContext dbContext,
    ILogger logger
)
{
    private static readonly int[] DaysCounts = [0, 1, 7];

    private static readonly Dictionary<int, string> DaysCountsAsStrings = new()
    {
        { 0, "сегодня" },
        { 1, "завтра" },
        { 7, "через неделю" }
    };

    public async Task CheckAndSendNotificationsAsync(CancellationToken ct = default)
    {
        foreach (int daysCount in DaysCounts)
        {
            var users = await GetUsersWithBirthdayAfterDaysCountAsync(dbContext, daysCount, ct);
            await SendNotificationsAsync(users, DaysCountsAsStrings[daysCount], ct);
        }
    }

    private async Task SendNotificationsAsync(IEnumerable<User> users,
        string timeToBirthdayString, CancellationToken ct = default)
    {
        foreach (var birthdayMan in users)
        {
            var subscriptions = birthdayMan.Profile!.SubscriptionsAsBirthdayMan;
            if (subscriptions is null)
            {
                continue;
            }

            foreach (var subscription in subscriptions)
            {
                var subscriber = subscription.Subscriber!.User;
                if (subscriber!.TelegramChatId is null)
                {
                    continue;
                }

                await bot.Client.SendTextMessageAsync(subscriber.TelegramChatId,
                    $"Напоминаю вам, что у пользователя {birthdayMan.UserName}" +
                    $"({birthdayMan.Surname} {birthdayMan.Name}) {timeToBirthdayString} будет день рождения! " +
                    $"Пожалуйста, подготовьте подарок и не забудьте поздравить именинника",
                    cancellationToken: ct
                );
                logger.LogInformation("Notification about {BirthdayManEmail}'s birthday " +
                                      "was sent to {SubscriberEmail}",
                    birthdayMan.UserName,
                    subscriber.UserName);
            }
        }
    }

    private static Task<List<User>> GetUsersWithBirthdayAfterDaysCountAsync(AppDbContext dbContext,
        int daysCount, CancellationToken ct = default)
        => dbContext.Users
            .Include(u => u.Profile)
            .ThenInclude(p => p!.SubscriptionsAsBirthdayMan)!
            .ThenInclude(s => s.Subscriber)
            .ThenInclude(p => p!.User!.TelegramChatId == null ? p.User : null)
            .Where(u => u.BirthDate.Month == DateTime.Today.Month
                        && u.BirthDate.Day == DateTime.Today.Day + daysCount)
            .ToListAsync(ct);
}