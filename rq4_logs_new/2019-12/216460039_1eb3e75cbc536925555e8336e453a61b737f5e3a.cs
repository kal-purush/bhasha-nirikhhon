using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using TwitchBot.Interfaces;
using TwitchBot.Models;
using TwitchBot.Services;

namespace TwitchBot.Commands
{
    public class RollCommand : ITwitchCommandObserver
    {
        private ILogger<RollCommand> _logger;
        private ITwitchCommandSubject _subject;
        private ITwitchIrcClientAdapter _client;
        private Task runner;

        private Regex NDMPattern = new Regex("^([0-9]+)d([0-9]+)([+-][0-9]+){0,1}$",RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private Regex DMPattern = new Regex("^d([0-9]+)([+-][0-9]+){0,1}$",RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private Regex MPattern = new Regex("^([0-9]+)([+-][0-9]+){0,1}$",RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public const string PrimaryCommand = "!roll";

        private Regex CommandRegex = new Regex("!roll|!r$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private BlockingCollection<TwitchChatCommand> queue = new BlockingCollection<TwitchChatCommand>(new ConcurrentQueue<TwitchChatCommand>());
        public RollCommand(
            ILogger<RollCommand> logger, 
            ITwitchCommandSubject subject, 
            ITwitchIrcClientAdapter client)
        {
            _logger = logger;
            _subject = subject;
            _client = client;

            subject.Attach(this);
            runner = Run();
        }

        public async Task Update(TwitchChatCommand Command)
        {
            await Task.Run(() => { queue.Add(Command); });
        }

        private Task Run()
        {
            return Task.Run(()=>
            {
                Task task;
                string message;
                foreach (var command in queue.GetConsumingEnumerable())
                {
                    _logger.LogInformation($"Processing {command.Command}...");
                    // Support: !roll ndm
                    // Support: !roll ndm+o
                    // Support: !roll ndm-o
                    // Support: !roll nDm
                    // Support: !roll Dm
                    // Support: !roll Dm+o
                    // Support: !roll Dm-o
                    // Support: !roll m
                    // Support: !roll m+o
                    // Support: !roll m-o
                    // support: !roll
                    int numberOfDice;
                    int maxDiceSides;
                    int offset;
                    if(!TryParseParameter(command, out numberOfDice, out maxDiceSides, out offset))
                    {
                        message = $"@{command.Message.TwitchUser.DisplayName} invalid command. Examples: '!roll 3d6', '!roll 3d6+5', '!roll 3d6-5', '!roll d6', '!roll 6'.";
                    }
                    else
                    {
                        var results = Roll(numberOfDice, maxDiceSides, offset);
                        message = $"@{command.Message.TwitchUser.DisplayName} Results: {results}";
                    }

                    task = _client.SendPublicChatMessageAsync(Message: message, Channel: command.Message.IrcChannel);
                }
            });
        }

        private string Roll(int numberOfDice, int maxDiceSides, int offset)
        {
            var random = new Random();
            var sb = new StringBuilder();
            int total = 0;
            for (int i = 0; i < numberOfDice; i++)
            {
                var roll = random.Next(1,maxDiceSides);
                total += roll;
                sb.Append(roll);
                sb.Append(" ");
            }
            total = total + offset;
            sb.Append("Total: ");
            sb.Append(total);

            return sb.ToString();
        }

        private bool TryParseParameter(TwitchChatCommand command, out int numberOfDice, out int maxDiceSides, out int offset)
        {
            offset = 0;
            numberOfDice = 0;
            maxDiceSides = 0;
            // match !roll
            if(!command.HasParameters)
            {
                numberOfDice = 1;
                maxDiceSides = 6;
                return true;
            }

            var Parameter = command.Parameters[0];
            // match !roll 3d6
            var match = NDMPattern.Match(Parameter);
            if(match.Success && int.TryParse(match.Groups[1].Value,out numberOfDice) && int.TryParse(match.Groups[2].Value, out maxDiceSides) && numberOfDice > 0 && maxDiceSides > 0)
            {
                if (match.Groups[3].Success && !int.TryParse(match.Groups[3].Value, out offset))
                {
                    return false;
                }
                return true;
            }

            // match !roll d6
            match = DMPattern.Match(Parameter);
            if(match.Success && int.TryParse(match.Groups[1].Value,out maxDiceSides) && maxDiceSides > 0)
            {
                numberOfDice = 1;
                if (match.Groups[2].Success && !int.TryParse(match.Groups[2].Value, out offset))
                {
                    return false;
                }
                return true;
            }

            // match !roll 6
            match = MPattern.Match(Parameter);
            if(match.Success && int.TryParse(match.Groups[1].Value,out maxDiceSides)  && maxDiceSides > 0)
            {
                numberOfDice = 1;
                if (match.Groups[2].Success && !int.TryParse(match.Groups[2].Value, out offset))
                {
                    return false;
                }
                return true;
            }

            numberOfDice = 0;
            maxDiceSides = 0;
            return false;
        }

        public string GetPrimaryCommand(TwitchChatCommand Command)
        {
            return PrimaryCommand;
        }

        public bool IsCommandSupported(string Command)
        {
            return CommandRegex.IsMatch(Command);
        }
    }
}