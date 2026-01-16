#nullable enable
using Daipan.Comment.MonoScripts;
using Daipan.Stream.Scripts.Utility;
using Daipan.Stream.Scripts.Utility.Scripts;

namespace Daipan.Comment.Scripts
{
    public class CommentPrefabLoader : IPrefabLoader<CommentMono>
    {
        readonly PrefabLoaderFromResources<CommentMono> _loader;

        public CommentPrefabLoader()
        {
            _loader = new PrefabLoaderFromResources<CommentMono>("Comment");
        }

        public CommentMono Load()
        {
            return _loader.Load();
        }
    }
}