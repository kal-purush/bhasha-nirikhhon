using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MVCswitchback.Models.Interfaces
{
    interface IUserCommentsManager
    {
        Task AddComment(UserComments comment);
    }
}