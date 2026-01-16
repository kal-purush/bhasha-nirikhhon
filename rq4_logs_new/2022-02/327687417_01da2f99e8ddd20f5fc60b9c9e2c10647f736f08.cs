namespace DiscordIntegration_Plugin.Patches
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection.Emit;
    using System.Threading.Tasks;
    using Exiled.API.Features;
    using HarmonyLib;
    using NorthwoodLib.Pools;
    using RemoteAdmin;

    using static HarmonyLib.AccessTools;

    [HarmonyPatch(typeof(CommandProcessor), nameof(CommandProcessor.ProcessQuery))]
    internal class CommandLogging
    {
        private static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions, ILGenerator generator)
        {
            List<CodeInstruction> newInstructions = ListPool<CodeInstruction>.Shared.Rent(instructions);
            const int index = 0;

            newInstructions.InsertRange(index, new[]
            {
                new CodeInstruction(OpCodes.Ldarg_0),
                new CodeInstruction(OpCodes.Ldarg_1),
                new CodeInstruction(OpCodes.Call, Method(typeof(CommandLogging), nameof(LogCommand))),
            });

            for (int z = 0; z < newInstructions.Count; z++)
                yield return newInstructions[z];

            ListPool<CodeInstruction>.Shared.Return(newInstructions);
        }

        private static void LogCommand(string query, CommandSender sender)
        {
            string[] args = query.Trim().Split(QueryProcessor.SpaceArray, 512, StringSplitOptions.RemoveEmptyEntries);
            if (args[0].ToUpperInvariant() == "REQUEST_DATA")
                return;

            Player player = sender is global::RemoteAdmin.PlayerCommandSender playerCommandSender
                ? Player.Get(playerCommandSender)
                : Server.Host;
            ProcessSTT.SendData($":keyboard: ({player.Id}) {sender.Nickname} ({player.UserId} - {player.Role}) {Plugin.Translation.UsedCommand}: {args[0]} {string.Join(" ", args.Where(a => a != args[0]))}", HandleQueue.CommandLogChannelId);
        }
    }
}