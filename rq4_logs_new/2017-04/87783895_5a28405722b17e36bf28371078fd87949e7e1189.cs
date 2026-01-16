using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace BHXH.Sys.Business
{
    class Settings
    {
        public static bool GetPasswordSetting()
        {
            try
            {
                XmlDocument doc = new XmlDocument();
                doc.Load("Settings.xml");
                XmlNodeList list = doc.GetElementsByTagName("UserID");
                foreach (XmlNode node in list)

                    if (node.InnerText.ToUpper() != string.Empty)
                        return true;
            }
            catch (Exception ex)
            {
                throw ex;

            }

            return false;

        }

        public static string GetSavedUserID()
        {
            try
            {
                XmlDocument doc = new XmlDocument();
                doc.Load("Settings.xml");
                XmlNodeList list = doc.GetElementsByTagName("UserID");
                foreach (XmlNode node in list)
                    return node.InnerText;
            }
            catch (Exception ex)
            {
                throw ex;

            }

            return string.Empty;

        }
        public static string GetSavePassword()
        {
            try
            {
                XmlDocument doc = new XmlDocument();
                doc.Load("Settings.xml");
                XmlNodeList list = doc.GetElementsByTagName("Password");
                foreach (XmlNode node in list)
                    return node.InnerText;
            }
            catch (Exception ex)
            {
                throw ex;

            }

            return string.Empty;

        }

        public static void SavePasswordSetting(string UserID, string EncryptedPassword)
        {

            try
            {
                XmlDocument doc = new XmlDocument();
                doc.Load("Settings.xml");
                XmlNodeList list = doc.GetElementsByTagName("UserID");
                foreach (XmlNode node in list)
                    node.InnerText = UserID;

                XmlNodeList list2 = doc.GetElementsByTagName("Password");
                foreach (XmlNode node in list2)
                    node.InnerText = EncryptedPassword;

                doc.Save("Settings.xml");
            }
            catch (Exception ex)
            {
                throw ex;

            }

        }
    }
}