using ChronoBot.Common.UserDatas;
using ChronoBot.Interfaces;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace ChronoBot.Common.Systems
{
    public class CountdownFileSystem : FileSystem
    {
        private const string ElementRoot = "Countdown";

        public sealed override string PathToSaveFile { get; set; }

        public CountdownFileSystem(string path = null)
        {
            if (string.IsNullOrEmpty(path))
                PathToSaveFile =
                    Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly()?.Location) ?? string.Empty,
                        @"Resources\Memory Card");
            else
                PathToSaveFile = path;

            if (!Directory.Exists(PathToSaveFile))
                Directory.CreateDirectory(PathToSaveFile);
        }

        public override bool Save(IUserData userData)
        {
            if (userData.GuildId == 0)
                return false;

            var countdownUserData = (CountdownUserData)userData;
            string guildId = countdownUserData.GuildId.ToString();
            string name = countdownUserData.Name ?? string.Empty;
            string channelId = countdownUserData.ChannelId.ToString();
            string id = countdownUserData.Id ?? string.Empty;
            string deadline = countdownUserData.Deadline.ToString(CultureInfo.InvariantCulture);
            string userId = countdownUserData.UserId.ToString();

            XElement user = new XElement("User");
            XAttribute newName = new XAttribute("Name", name);
            XAttribute newChannelId = new XAttribute("ChannelID", channelId);
            XAttribute newId = new XAttribute("ID", id);
            XAttribute newDeadline = new XAttribute("Deadline", deadline);
            XAttribute newUserId = new XAttribute("UserID", userId);
            user.Add(newName, newChannelId, newId, newDeadline, newUserId);

            XDocument xDoc;
            string guildPath = Path.Combine(PathToSaveFile, guildId + ".xml");
            if (!File.Exists(guildPath))
            {
                xDoc = new XDocument();

                XElement sm = new XElement(ElementRoot);
                sm.Add(user);

                XElement root = new XElement("Service");
                root.Add(sm);

                xDoc.Add(root);
            }
            else
            {
                xDoc = XDocument.Load(guildPath);

                if (xDoc.Root != null)
                {
                    XElement sm = xDoc.Root.Element(ElementRoot);
                    if (sm == null)
                    {
                        sm = new XElement(ElementRoot);
                        sm.Add(user);
                        xDoc.Root.Add(sm);
                    }
                    else
                        xDoc.Root.Element(ElementRoot)?.Add(user);
                }
            }

            xDoc.Save(guildPath);

            return true;
        }

        public override IEnumerable<IUserData> Load()
        {
            Dictionary<XDocument, ulong> xmls = new Dictionary<XDocument, ulong>();
            DirectoryInfo dirInfo = new DirectoryInfo(PathToSaveFile);
            foreach (FileInfo fi in dirInfo.GetFiles())
            {
                if (fi.FullName.EndsWith(".xml"))
                {
                    try
                    {
                        xmls.Add(XDocument.Load(fi.FullName), ulong.Parse(Path.GetFileNameWithoutExtension(fi.Name)));
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        StackTrace st = new StackTrace();
                        MethodBase mb = st.GetFrame(1).GetMethod();
                    }
                }
            }

            if (xmls.Count == 0)
                return new List<CountdownUserData>();

            List<CountdownUserData> ud = new List<CountdownUserData>();
            ud.AddRange(CollectUserData(xmls));
            return ud;
        }

        private IEnumerable<CountdownUserData> CollectUserData(Dictionary<XDocument, ulong> xmls)
        {
            List<CountdownUserData> ud = new List<CountdownUserData>();
            foreach (KeyValuePair<XDocument, ulong> xml in xmls)
            {
                foreach (XElement e in xml.Key.Descendants("Service").Descendants(ElementRoot).Descendants("User"))
                {
                    CountdownUserData user = new CountdownUserData();
                    if (xml.Key.Document != null)
                        user.GuildId = xml.Value;
                    user.Name = e.Attribute("Name")?.Value;
                    user.ChannelId = ulong.Parse(e.Attribute("ChannelID")?.Value ?? string.Empty);
                    user.Id = e.Attribute("ID")?.Value;
                    user.Deadline = DateTime.Parse(e.Attribute("Deadline")?.Value ?? string.Empty);
                    user.UserId = ulong.Parse(e.Attribute("UserID")?.Value ?? string.Empty);
                    ud.Add(user);
                }
            }
            return ud;
        }

        public override bool UpdateFile(IUserData userData)
        {
            if (!(userData is CountdownUserData countdownUserData))
                return false;

            string guildPath = Path.Combine(PathToSaveFile, countdownUserData.GuildId + ".xml");
            if (!File.Exists(guildPath))
                return false;

            var xml = XDocument.Load(guildPath);
            List<CountdownUserData> users = new List<CountdownUserData>();
            users.AddRange(CollectUserData(new Dictionary<XDocument, ulong> { { xml, countdownUserData.GuildId } }));
            bool updated = false;
            foreach (CountdownUserData ud in users)
            {
                if (ud.Name == countdownUserData.Name)
                {
                    try
                    {
                        XElement found = xml.Descendants("Service").Descendants(ElementRoot).Descendants("User")
                            .First(x => x.Attributes("UserID").First().Value == countdownUserData.UserId.ToString());
                        found.Attributes("Name").First().Value = countdownUserData.Name;
                        found.Attributes("ChannelID").First().Value = countdownUserData.ChannelId.ToString();
                        found.Attributes("ID").First().Value = countdownUserData.Id;
                        found.Attributes("Deadline").First().Value = countdownUserData.Deadline.ToShortDateString();
                        found.Attributes("UserID").First().Value = countdownUserData.UserId.ToString();
                        updated = true;
                        break;
                    }
                    catch
                    {
                        return false;
                    }
                }
            }

            if (!updated)
                return false;

            xml.Save(guildPath);

            return true;
        }

        public override bool DeleteInFile(IUserData userData)
        {
            if (!(userData is CountdownUserData countdownUserData))
                return false;

            string guildPath = Path.Combine(PathToSaveFile, countdownUserData.GuildId + ".xml");
            if (!File.Exists(guildPath))
            {
                Console.WriteLine($"Unable to delete {0} \"{1}\"", countdownUserData.Name, countdownUserData.Id);
                return false;
            }

            XDocument xml = XDocument.Load(guildPath);
            List<CountdownUserData> users = new List<CountdownUserData>();
            users.AddRange(CollectUserData(new Dictionary<XDocument, ulong> { { xml, countdownUserData.GuildId } }));
            bool remove = true;
            foreach (CountdownUserData ud in users)
            {
                if (ud.Id != countdownUserData.Id && ud.UserId != countdownUserData.UserId)
                    continue;
                xml.Descendants("Service")
                    .Descendants(ElementRoot)
                    .Descendants("User").Where(x => x.Attribute("ID")?.Value == ud.Id && x.Attribute("UserID")?.Value == ud.UserId.ToString())
                    .Remove();
                remove = false;
                break;
            }

            if (remove)
                return false;

            xml.Save(guildPath);

            return true;
        }
    }
}