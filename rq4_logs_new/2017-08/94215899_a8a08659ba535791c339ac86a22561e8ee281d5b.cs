using System;
using System.Linq;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using HashtagAggregator.Core.Entities.VkEntities;
using HashtagAggregator.Shared.Common.Infrastructure;
using HashtagAggregatorConsumer.Models;
using HashTagAggregatorConsumer.Queries.Cqrs.Commands;
using HashtagAggregator.Shared.Contracts.Enums;

namespace HashTagAggregatorConsumer.Data.Vk.Mappers
{
    public class VkMessageResultMapper
    {
        private const string HashtagParseRegexString = @"#(\w+)";

        private readonly Regex hashtagParseRegex =
            new Regex(HashtagParseRegexString, RegexOptions.Compiled | RegexOptions.Multiline);

        public List<CreateMessageCommand> MapBunch(VkNewsFeed feed)
        {
            var results = new List<CreateMessageCommand>();

            // vk doesn't return list of all hashtags in message
            // todo: consider parsing message for hashtags. But it might be too memory consuming operation.

            foreach (var post in feed.Feed)
            {
                var date = DateTimeOffset.FromUnixTimeSeconds(post.UnixTimeStamp).UtcDateTime;
                var user = FillUser(post, feed);

                var message = new CreateMessageCommand
                {
                    MessageText = post.Text,
                    HashTags = ParseHashtags(post.Text).Select(x => new HashtagModel
                    {
                        HashTag = new HashTagWord(x),
                        IsEnabled = false
                    }).ToList(),
                    MediaType = SocialMediaType.VK,
                    PostDate = date,
                    NetworkId = post.Id.ToString(),
                    User = user
                };
                results.Add(message);
            }
            return results;
        }

        private List<string> ParseHashtags(string body)
        {
            var tags = new List<string>();
            if (!String.IsNullOrWhiteSpace(body))
            {
                tags = hashtagParseRegex.Matches(body)
                    .Select(m => m.Groups[1].Value)
                    .ToList();
            }
            return tags;
        }

        private UserModel FillUser(VkNewsSearchResult post, VkNewsFeed feed)
        {
            var id = post.FromId;
            var user = new UserModel();
            if (id > 0)
            {
                var profile = feed.Profiles.FirstOrDefault(x => x.Id == id);
                user.NetworkId = id.ToString();
                user.UserName = $"{profile.FirstName} {profile.LastName}";
                user.ProfileId = profile.UserName;
                user.AvatarUrl50 = profile.PhotoLink50;
            }
            else
            {
                var vkGroup = feed.Groups.FirstOrDefault(x => x.Id == Math.Abs(id));
                user.NetworkId = id.ToString();
                user.UserName = $"{vkGroup.FirstName}";
                user.ProfileId = vkGroup.UserName;
                user.AvatarUrl50 = vkGroup.PhotoLink50;
            }
            return user;
        }
    }
}