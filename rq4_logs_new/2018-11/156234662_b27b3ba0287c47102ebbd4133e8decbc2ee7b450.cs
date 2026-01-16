using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FacebookApp
{
    public class FacebookUtils
    {
        private List<UserNode> m_UserList;

        public List<UserNode> UserList { get { return m_UserList; } }

        public FacebookUtils()
        {
            m_UserList = new List<UserNode>();
        }

        public void AddFriend(UserNode user)
        {
            m_UserList.Add(user);
        }

       public void sortFriendsByDate()
        {
            m_UserList.Sort((user1, user2) => Comparer<DateTime?>.Default.Compare(user1.Birthday, user2.Birthday));
        }
    }
}