using CommunityNetwork.Common.Inerfaces;
using CommunityNetwork.Common.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Social.BL.Interfaces
{
    public interface IPublisher
    {
        TPublish Publish<TPublish>(string profileId, TPublish publish)
            where TPublish : IPost;
         Comment Comment<TCommentable>(string profileId, Comment comment, string commentableId)
            where TCommentable : IPost;

    }
}