using digioz.Forum.Services;
using digioz.Forum.Services.Interfaces;
using Microsoft.Data.SqlClient;
using NuGet.ProjectModel;

namespace digioz.Forum.Helpers
{
    public class ForumAuthHelper
    {
        private Dictionary<long, Dictionary<string, bool>> _acl;
        private Dictionary<long, Dictionary<string, bool>> _cache;
        private Dictionary<string, Dictionary<string, long>> _aclOptions;
        private List<long> _aclForumIds;

        private readonly IForumService _forumService;
        private readonly IForumAclOptionService _forumAclOptionService;

        public ForumAuthHelper(IForumService forumService, IForumAclOptionService forumAclOptionService)
        {
            _acl = new Dictionary<long, Dictionary<string, bool>>();
            _cache = new Dictionary<long, Dictionary<string, bool>>();
            _aclOptions = new Dictionary<string, Dictionary<string, long>>();
            _aclForumIds = null;
            _forumService = forumService;
            _forumAclOptionService = forumAclOptionService;
        }

        public void Acl(ref Dictionary<string, object> userdata)
        {
            _acl = new Dictionary<long, Dictionary<string, bool>>();
            _cache = new Dictionary<long, Dictionary<string, bool>>();
            _aclOptions = new Dictionary<string, Dictionary<string, long>>();
            _aclForumIds = null;

            if (_aclOptions == null)
            {
                _aclOptions = new Dictionary<string, Dictionary<string, long>>();
                var aclOptions = _forumAclOptionService.GetAll();

                int global = 0;
                int local = 0;
                foreach (var option in aclOptions)
                {
                    if (option.IsGlobal == 1)
                    {
                        if (!_aclOptions.ContainsKey("global"))
                        {
                            _aclOptions["global"] = new Dictionary<string, long>();
                        }
                        _aclOptions["global"][option.AuthOption] = global++;
                    }
                    if (option.IsLocal == 1)
                    {
                        if (!_aclOptions.ContainsKey("local"))
                        {
                            _aclOptions["local"] = new Dictionary<string, long>();
                        }
                        _aclOptions["local"][option.AuthOption] = local++;
                    }
                }
            }
        }

        public Dictionary<long, Dictionary<string, bool>> AclGetF(string opt, bool clean = false)
        {
            var aclF = new Dictionary<long, Dictionary<string, bool>>();
            bool negate = false;

            if (opt.StartsWith("!"))
            {
                negate = true;
                opt = opt.Substring(1);
            }

            // If we retrieve a list of forums not having permissions in, we need to get every forum_id
            if (negate)
            {
                if (_aclForumIds == null)
                {
                    _aclForumIds = _forumService.GetForumIds();
                }

                foreach (var forumId in _aclForumIds)
                {
                    if (!_acl.ContainsKey(forumId) || !_acl[forumId].ContainsKey(opt) || !_acl[forumId][opt])
                    {
                        if (!aclF.ContainsKey(forumId))
                        {
                            aclF[forumId] = new Dictionary<string, bool>();
                        }
                        aclF[forumId][opt] = false;
                    }
                }
            }
            else
            {
                foreach (var forum in _acl)
                {
                    if (forum.Value.ContainsKey(opt) && forum.Value[opt])
                    {
                        if (!aclF.ContainsKey(forum.Key))
                        {
                            aclF[forum.Key] = new Dictionary<string, bool>();
                        }
                        aclF[forum.Key][opt] = true;
                    }
                }
            }

            return aclF;
        }
    }
}