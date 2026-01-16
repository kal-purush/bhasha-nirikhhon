using CommunityNetwork.Common;
using CommunityNetwork.Common.Inerfaces;
using CommunityNetWork.Dal.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Identity.BL
{
    public class IdentityManager: IIdentityService
    {
        private readonly IDynamoDB _iDynamoDB;
        private readonly IDBConnectorFactory dBFactory;

        public IdentityManager(IDBConnectorFactory dBFactory)
        {
            this.dBFactory = dBFactory;
            this._iDynamoDB = (IDynamoDB)dBFactory.Create(false);
        }

        public Profile Edit(Profile profile)
        {
            var profileConnected = _iDynamoDB.Get<Profile>(profile.Email, true);
            profileConnected.FirstName = profile.FirstName;
            profileConnected.LastName = profile.LastName;
            profileConnected.Password = profile.Password;
            profileConnected.WorkPlace = profile.WorkPlace;
            profileConnected.UserName = profile.UserName;
            profileConnected.BirthDate = profile.BirthDate;
            _iDynamoDB.Add(profileConnected);
            return profileConnected;
        }
    }
}