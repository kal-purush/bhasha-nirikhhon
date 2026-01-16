using digioz.Forum.Models;

namespace digioz.Forum.Helpers
{
    public class UserHelper
    {
        private readonly IHttpContextAccessor _httpContextAccessor;

        public UserHelper(IHttpContextAccessor httpContextAccessor)
        {
            _httpContextAccessor = httpContextAccessor;
        }

        public string GetUserId()
        {
            var userId = "";
            var user = _httpContextAccessor.HttpContext?.User?.Identity?.Name;
            if (user != null)
            {
                userId = user;
            }
            return userId;
        }

        public int GetUserType(string name)
        {
            var userType = 0;

            if (name == "USER_NORMAL")
            {
                userType = 0;
            }
            else if (name == "USER_INACTIVE")
            {
                userType = 1;
            }
            else if (name == "USER_IGNORE")
            {
                userType = 2;
            }
            else if (name == "USER_FOUNDER")
            {
                userType = 3;
            }
            else
            {
                userType = 0;
            }

            return userType;
        }

        public ForumUser GetDefaultUser()
        {
            var dateStamp = DateTime.Now.Ticks;

            var user = new ForumUser()
            {
                ForumUserId = 0,
                UserId = string.Empty,
                UserType = 0,
                GroupId = 2,
                UserPermissions = string.Empty,
                UserPermFrom = 0,
                UserIp = string.Empty,
                UserRegdate = dateStamp,
                UserName = string.Empty,
                UserNameClean = string.Empty,
                UserEmail = string.Empty,
                UserBirthday = string.Empty,
                UserLastVisit = dateStamp,
                UserLastActive = dateStamp,
                UserLastMark = dateStamp,
                UserLastPostTime = 0,
                UserLastPage = string.Empty,
                UserLastConfirmKey = string.Empty,
                UserLastSearch = 0,
                UserWarnings = 0,
                UserLastWarning = 0,
                UserLoginAttempts = 0,
                UserInactiveReason = 0,
                UserInactiveTime = 0,
                UserPosts = 0,
                UserLang = "en",
                UserTimeZone = "UTC",
                UserDateFormat = "D M d, Y g:i a",
                UserStyle = 1,
                UserRank = 0,
                UserColor = string.Empty,
                UserNewPrivmsg = 0,
                UserUnreadPrivmsg = 0,
                UserLastPrivmsg = 0,
                UserMessageRules = 0,
                UserFullFolder = -3,
                UserEmailTime = 0,
                UserTopicShowDays = 0,
                UserTopicSortbyType = "t",
                UserTopicSortbyDir = "d",
                UserPostShowDays = 0,
                UserPostSortbyType = "t",
                UserPostSortbyDir = "a",
                UserNotify = 0,
                UserNotifyPm = 1,
                UserNotifyType = 0,
                UserAllowPm = 1,
                UserAllowViewOnline = 1,
                UserAllowViewEmail = 1,
                UserAllowMassemail = 1,
                UserOptions = 230271,
                UserAvatar = string.Empty,
                UserAvatarType = string.Empty,
                UserAvatarWidth = 0,
                UserAvatarHeight = 0,
                UserSig = string.Empty,
                UserSigBbcodeUid = string.Empty,
                UserSigBbcodeBitfield = string.Empty,
                UserJabber = string.Empty,
                UserActkey = string.Empty,
                UserActkeyExpiration = 0,
                ResetToken = string.Empty,
                ResetTokenExpiration = 0,
                UserNewpasswd = string.Empty,
                UserFormSalt = string.Empty,
                UserNew = 1,
                UserReminded = 0,
                UserRemindedTime = 0
            };
            return user;
        }
    }
}