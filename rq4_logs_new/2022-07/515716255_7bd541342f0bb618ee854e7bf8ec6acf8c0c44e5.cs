using Discord.Interactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Discord;
using Dynastio.Net;
using Discord.WebSocket;

namespace Dynastio.Bot.Interactions.SlashCommands
{
    [RequireContext(ContextType.Guild)]
    [RequireBotPermission(ChannelPermission.SendMessages | ChannelPermission.EmbedLinks)]
    [Group("videos", "dynastio videos")]
    public class Videos : CustomInteractionModuleBase<CustomSocketInteractionContext>
    {
        [Group("featured", "featured videos")]
        public class FeaturedVideosModule : CustomInteractionModuleBase<CustomSocketInteractionContext>
        {
            public DynastioClient Dynastio { get; set; }

            [RateLimit(2)]
            [SlashCommand("random", "get a random video")]
            [ComponentInteraction("videos.featured.random:*:*", true)]
            public async Task random(LocalType local = LocalType.common, DynastioProviderType provider = DynastioProviderType.Main)
            {
                await DeferAsync();

                var videos = Dynastio[provider].FeaturedVideos.Where(a => a.Group == local.ToString()).ToList();
                if (videos == null || videos.Count == 0)
                {
                    await FollowupAsync(embed: "No video found.".ToWarnEmbed("not found"));
                    return;
                }
                var index = Program.Random.Next(videos.Count);
                var video = videos[index];

                var componenets = new ComponentBuilder();
                componenets.WithButton(this["next"], $"videos.featured.random:{local}:{provider}", ButtonStyle.Success, new Emoji("⏩"), null, false, 0);

                var message = await FollowupAsync(video.Url, components: componenets.Build());
            }
            public enum LocalType { ru, common }

            [RateLimit(5)]
            [SlashCommand("get", "get a list of videos")]
            public async Task get(int skip = 0, [MaxValue(50)] int take = 50, DynastioProviderType provider = DynastioProviderType.Main)
            {
                await DeferAsync();
                var videos = Dynastio[provider].FeaturedVideos.ToList();
                if (videos == null || videos.Count == 0)
                {
                    await FollowupAsync(embed: "No video found.".ToWarnEmbed("not found"));
                    return;
                }
                var content = $"**Totall Featured Videos**: {videos.Count}\n";
                videos = videos.Skip(skip).Take(take).ToList();
                videos.ForEach(a =>
                {
                    content +=
                    $"Url: {a.Url}\n" +
                    $"ExpireAt: {a.ExpireAt.ToDiscordUnixTimestampFormat()}\n\n";
                });
                await FollowupAsync(embed: content.ToEmbed("Videos"));
            }

        }
    }
}