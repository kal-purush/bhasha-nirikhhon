using System;
using System.Threading.Tasks;
using DSharpPlus;
using DSharpPlus.Entities;
using DSharpPlus.CommandsNext;
using DSharpPlus.CommandsNext.Attributes;
using System.Linq;
using ModBot.Models;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using DSharpPlus.Interactivity.Extensions;

namespace ModBot.Commands
{
    [RequireGuild]
    public class ReactionPins : Cog
    {
        public dataContext context { private get; set; }

        [Group("setpinperms")]
        [Description("Allow/Disallow roles to pin messages with the 📌 react")]
        [RequireUserPermissions(Permissions.ManageMessages)]
        public class SetPinPerms : BaseCommandModule
        {
            public dataContext context { private get; set; }

            [Command("allowed")]
            [Description("Allow a role to pin message with the 📌 react")]
            public async Task Allowed(CommandContext ctx, [Description("Role that will be allowed to pin")] DiscordRole role)
            {
                if (!ctx.Guild.Roles.Any(r => r.Value.Id == role.Id)) return;
                if (context.Pinners.Any(c => c.Roleid == role.Id.ToString()))
                    throw new UserError("That role is already allowed");
                await context.AddAsync(new Pinner { Roleid = role.Id.ToString(), Guild = ctx.Guild.Id.ToString() });
                await context.SaveChangesAsync();
                await ctx.RespondAsync(embed: Embeds.Success.WithDescription($"Allowed {role.Mention} to pin messages with the 📌 react!"));
            }
            [Command("disallowed")]
            [Description("Disallow a role to pin message with the 📌 react")]
            public async Task Disallowed(CommandContext ctx, [Description("Role that will be disallowed from pinning")] DiscordRole role)
            {
                if (!ctx.Guild.Roles.Any(r => r.Value.Id == role.Id)) return;
                if (context.Pinners.Any(c => c.Roleid == role.Id.ToString()))
                    throw new UserError("That role is already disallowed");
                context.Remove(context.Pinners.Single(c => c.Roleid == role.Id.ToString()));
                await context.SaveChangesAsync();
                await ctx.RespondAsync(embed: Embeds.Success.WithDescription($"Disallowed {role.Mention} from pinning messages with the 📌 react!"));
            }
        }
        [Command("listpinperms")]
        [Description("List all roles with pin permissions")]
        public async Task ListPinPerms(CommandContext ctx)
        {
            var pinRoles = string.Join("\n", context.Pinners.Where(c => c.Guild == ctx.Guild.Id.ToString())
                 .Select(u => $"<@&{u.Roleid}>"));
            if (pinRoles.Length == 0) pinRoles = "*There are no pin roles enabled in this server*";
            await ctx.RespondAsync(embed: Embeds.Info.WithTitle("Roles With 📌 Permission").WithDescription(pinRoles));
        }
#pragma warning disable 4014
        public static void OnStart(DiscordClient discord, IConfiguration configuration)
        {
            discord.MessageReactionAdded += async (client, args) =>
            {
                Task.Run(async () =>
                {
                    try
                    {
                        var message = await args.Channel.GetMessageAsync(args.Message.Id);
                        if (args.Emoji != DiscordEmoji.FromUnicode("📌") || message.Pinned) return;
                        var context = new dataContext();
                        var validPinners = context.Pinners.Where(r => r.Guild == args.Guild.Id.ToString()).Select(r => r.Roleid);
                        if ((args.User as DiscordMember).Roles.Any(r => validPinners.Contains(r.Id.ToString())))
                        {
                            // TODO: Delete pinned by ModBot message
                            await message.PinAsync();
                            var lastMessage = (await message.Channel.GetMessagesAsync()).FirstOrDefault(m => m.MessageType == MessageType.ChannelPinnedMessage);
                            if (lastMessage != null) await lastMessage.DeleteAsync();
                            await args.Channel.SendMessageAsync(embed: new DiscordEmbedBuilder()
                                .WithDescription($"{args.User.Mention} pinned [a message]({args.Message.JumpLink}) to this channel"));
                        }
                    }
                    catch
                    {
                        await args.Channel.SendMessageAsync(embed: Embeds.Error.WithDescription("Failed to pin. There might be too many messages pinned already"));
                    }
                });
            };
            discord.MessageReactionRemoved += async (client, args) =>
            {
                Task.Run(async () =>
                {
                    var message = await args.Channel.GetMessageAsync(args.Message.Id);
                    if (args.Emoji != DiscordEmoji.FromUnicode("📌") || !message.Pinned) return;
                    var context = new dataContext();
                    var validPinners = context.Pinners.Where(r => r.Guild == args.Guild.Id.ToString()).Select(r => r.Roleid);
                    if ((args.User as DiscordMember).Roles.Any(r => validPinners.Contains(r.Id.ToString())))
                    {
                        await message.UnpinAsync();
                        await args.Channel.SendMessageAsync(embed: new DiscordEmbedBuilder()
                            .WithDescription($"{args.User.Mention} unpinned [a message]({args.Message.JumpLink}) from this channel"));
                    }
                });
            };
        }
    }
#pragma warning restore 4014
}