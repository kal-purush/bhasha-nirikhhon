using System.Collections.Generic;
using CoreCodedChatbot.ApiApplication.Interfaces.Repositories.Vip;
using CoreCodedChatbot.ApiContract.Enums.VIP;
using CoreCodedChatbot.ApiContract.RequestModels.Vip.ChildModels;
using CoreCodedChatbot.Database.Context.Interfaces;
using CoreCodedChatbot.Database.DbExtensions;

namespace CoreCodedChatbot.ApiApplication.Repositories.Vip
{
    public class GiveSubVipsRepository : IGiveSubVipsRepository
    {
        private readonly IChatbotContextFactory _chatbotContextFactory;

        public GiveSubVipsRepository(
            IChatbotContextFactory chatbotContextFactory)
        {
            _chatbotContextFactory = chatbotContextFactory;
        }

        public void Give(List<UserSubDetail> userSubDetails, int tier2ExtraVips, int tier3ExtraVips)
        {
            using (var context = _chatbotContextFactory.Create())
            {
                foreach (var userSubDetail in userSubDetails)
                {
                    var user = context.GetOrCreateUser(userSubDetail.Username);

                    user.SubVipRequests = userSubDetail.TotalSubMonths;
                    switch (userSubDetail.SubscriptionTier)
                    {
                        case SubscriptionTier.Tier2:
                            user.Tier2Vips += tier2ExtraVips;
                            break;
                        case SubscriptionTier.Tier3:
                            user.Tier3Vips += tier3ExtraVips;
                            break;
                        default:
                            break;
                    }
                }

                context.SaveChanges();
            }
        }
    }
}