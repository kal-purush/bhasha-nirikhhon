using HDV.FacebookSDK.Game;
using HDV.FacebookSDK.GraphAPI;
using HDV.FacebookSDK.Login;
using HDV.FacebookSDK.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HDV.FacebookSDK
{
    /// <summary>
    /// Giao diện giao tiếp cấp cao với Facebook SDK
    /// </summary>
    public class FacebookClient
    {
        private static FacebookClient instance;

        /// <summary>
        /// Phân cách quyền truy cập
        /// </summary>
        private const char PERMISSION_SEPARATOR = ',';

        private GameService m_GameService;

        private FacebookClient()
        {
        }

        /// <summary>
        /// Instance duy nhất của Facebook CLient
        /// </summary>
        public static FacebookClient Current
        {
            get
            {
                if (instance == null)
                    instance = new FacebookClient();

                return instance;
            }
        }

        private FacebookSession currentSession;
        
        /// <summary>
        /// Lấy session đăng nhập hiện hành
        /// </summary>
        public FacebookSession CurrentSession
        {
            get
            {
                return currentSession;
            }
        }

        /// <summary>
        /// Trả ra trạng thái truy cập hiện tại
        /// </summary>
        public bool IsLoggedIn
        {
            get
            {
                return currentSession != null;
            }
        }

		#if ANDROID
		public async Task<FacebookSession> LoginAsync(Android.Content.Context context, string appid, params Permission[] permissions)
		#else
        /// <summary>
        /// Đăng nhập Facebook bất đồng bộ
        /// </summary>
        /// <param name="appid">Facebook App ID của ứng dụng</param>
        /// <returns></returns>
        public async Task<FacebookSession> LoginAsync(string appid, params Permission[] permissions)
		#endif
        {
            StringBuilder permissionsString = new StringBuilder();

            if (permissions != null && permissions.Length > 0)
            {
                foreach (var permission in permissions)
                {
                    if (permissionsString.Length > 0)
                        permissionsString.Append(PERMISSION_SEPARATOR);

                    permissionsString.Append(permission.ToString());
                }
            }

			#if ANDROID
            string accessToken = await FacebookLogin.Login(context, appid, permissionsString.ToString());
			#else
			string accessToken = await FacebookLogin.Login(appid, permissionsString.ToString());
			#endif

            if (string.IsNullOrEmpty(accessToken))
                return null;

            currentSession = new FacebookSession
            {
                AccessToken = accessToken,
                AppID = appid,
            };

            return currentSession;
        }

        #region User Info API
        /// <summary>
        /// Lấy thông tin của tài khoản đang sử dụng API
        /// </summary>
        /// <param name="fields">Các trường cần lấy</param>
        /// <returns></returns>
        public async Task<GraphUser> GetAboutMeAsync(params string[] fields)
        {
            return await this.GetAboutUserAsync("me", fields);
        }

        /// <summary>
        /// Lấy thông tin của một người dùng
        /// </summary>
        /// <param name="userId">Id của người dùng</param>
        /// <param name="fields">Các trường muốn lấy</param>
        /// <returns></returns>
        public async Task<GraphUser> GetAboutUserAsync(string userId, params string[] fields)
        {
            Request request = Request.CreateGetUserInfoRequest(currentSession, userId, fields);
            var respone = await request.ExecuteAsync();
            if (respone.RequestError != null)
            {
                throw new GraphAPIRequestException(respone.RequestError);
            }

            return respone.GraphObject.Cast<GraphUser>();
        }

        public async Task<Picture> GetUserProfilePictureAsync(string userId, bool isRedirect = false, 
            PictureType type = PictureType.Normal, int width = 0, int height = 0)
        {
            Request request = Request.CreateGetUserPropfilePictureRequest(currentSession, 
                userId, isRedirect, type, width, height);

            var respone = await request.ExecuteAsync();
            if (respone.RequestError != null)
            {
                throw new GraphAPIRequestException(respone.RequestError);
            }

            return respone.GraphObject.Cast<Picture>();
        }

        public async Task<Picture> GetMyProfilePictureAsync(bool isRedirect = false, 
            PictureType type = PictureType.Normal, int width = 0, int height = 0)
        {
            return await GetUserProfilePictureAsync("me", isRedirect, type, width, height);
        }

        public async Task<List<GraphUser>> GetFriendListOfUser(string userId, params string[] fields)
        {
            Request request = Request.CreateGetFriendListOfUserRequest(currentSession, userId, fields);
            var respone = await request.ExecuteAsync();
            if (respone.RequestError != null)
            {
                throw new GraphAPIRequestException(respone.RequestError);
            }

            List<GraphUser> result = new List<GraphUser>();
            if (respone.IsPagingResult && respone.GraphObjectList != null)
            {
                var graphObjectList = respone.GraphObjectList;
                foreach (var graphObject in graphObjectList)
                {
                    result.Add(graphObject.Cast<GraphUser>());
                }
            }

            return result;
        }

        public async Task<List<GraphUser>> GetMyFriendList(params string[] fields)
        {
            return await GetFriendListOfUser("me", fields);
        }
        
        public async Task<List<GraphUser>> GetTaggableFriendListOfUser(string userId, params string[] fields)
        {
            Request request = Request.CreateGetTaggableFriendListOfUserRequest(currentSession, userId, fields);
            var respone = await request.ExecuteAsync();
            if (respone.RequestError != null)
            {
                throw new GraphAPIRequestException(respone.RequestError);
            }

            List<GraphUser> result = new List<GraphUser>();

            if (respone.IsPagingResult && respone.GraphObjectList != null)
            {
                var graphObjectList = respone.GraphObjectList;
                foreach (var graphObject in graphObjectList)
                {
                    result.Add(graphObject.Cast<GraphUser>());
                }
            }

            return result;
        }

        public async Task<List<GraphUser>> GetMyTaggableFriendList(params string[] fields)
        {
            return await GetTaggableFriendListOfUser("me", fields);
        }
        #endregion

        public GameService GameService
        {
            get
            {
                if (m_GameService == null)
                    m_GameService = new GameService();

                return m_GameService;
            }
        }

        /// <summary>
        /// Đăng xuất phiên làm việc hiện tại
        /// </summary>
        public async Task LogoutAsync()
        {
            if (currentSession == null)
                return;

            await FacebookLogin.Logout(currentSession.AccessToken);

            this.currentSession = null;
        }

        
    }
}