using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Facebook;
using FacebookWrapper;
using FacebookWrapper.ObjectModel;

namespace FacebookApp
{
    public class Session
    {
        private AppSettings m_AppSettings;
        private LoginResult m_Result;

        public AppSettings AppSettings { get { return m_AppSettings; } set {; } }

        public LoginResult Result { get { return m_Result; } set {; } }

        public Session()
        {
            m_AppSettings = new AppSettings();
            m_Result = new LoginResult();
        }

        public void LoginAndInit()
        {
            if (!m_AppSettings.RememberUser)
            {
                m_Result = FacebookService.Login("1450160541956417", "public_profile", "user_friends", "user_birthday", "user_photos");
                // m_Result = FacebookService.Login("805313746467364", "public_profile", "user_friends", "user_birthday", "user_photos");                
            }
            else
            {
                m_Result = FacebookService.Connect(m_AppSettings.LastAccessToken);
            }
        }

        public void CloseApp()
        {
            if (m_AppSettings.RememberUser)
            {
                m_AppSettings.LastAccessToken = m_Result.AccessToken;
            }
            else
            {
                m_AppSettings.LastAccessToken = null;
            }

            m_AppSettings.SaveToFile();
        }

        public void AppOpened()
        {
            string filePath = m_AppSettings.FullPath;
            if (File.Exists(filePath))
            {
                m_AppSettings = AppSettings.LoadFromFile();
                if (m_AppSettings.RememberUser)
                {
                   LoginAndInit();
                }
            }
        }
    }
}