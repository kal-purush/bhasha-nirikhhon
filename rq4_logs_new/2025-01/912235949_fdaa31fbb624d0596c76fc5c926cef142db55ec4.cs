namespace ConfigRefs
{
    using UnityEngine;

    [System.Serializable]
    public struct ConfigReference<T> where T : ScriptableObject
    {
        public string Guid;
        public string ObjName; 

#if UNITY_EDITOR
        public static ConfigReference<T> CreateFromObject(Object obj)
        {
            if (obj == null)
            {
                return default;
            }

            var assetPath = UnityEditor.AssetDatabase.GetAssetPath(obj);
                assetPath = assetPath.ToLowerInvariant();

            var configReference = new ConfigReference<T>()
            {
                ObjName = obj.name,
            };

            if (UnityEditor.AssetDatabase.TryGetGUIDAndLocalFileIdentifier(obj, out string guid, out long fileID))
            {
                configReference.Guid = guid;
            }

            return configReference;
        }
#endif

        public ConfigReference(string guid, string name)
        {
            Guid = guid;
            ObjName = name;

            _hashCache = Guid.GetHashCode();
        }

        public static readonly ConfigReference<T> Invalid = new ConfigReference<T>(string.Empty, string.Empty);

        public override string ToString()
        {
            return Guid;
        }

        public override int GetHashCode()
        {
            if (string.IsNullOrEmpty(Guid))
            {
                return -1;
            }

            if (_hashCache == 0)
            {
                _hashCache = Guid.GetHashCode();
            }

            return _hashCache;
        }

        public bool IsValid()
        {
            return Guid != null && Guid != Invalid.Guid;
        }

        public bool IsValidAndNonNull()
        {
            if (Guid != null && Guid != Invalid.Guid)
            {
                var baseConfig = TryGetBaseConfig();
                return baseConfig != null;
            }

            return false;
        }

        [System.NonSerialized] private int _hashCache;

        /// <summary>
        /// Returns the base config, but as another type (only valid if its a child of the original type)
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <returns></returns>
        public U TryGetBaseConfig<U>() where U : T
        {
            var baseScriptableObject = TryGetBaseConfig();
            return baseScriptableObject as U;
        }

        public T TryGetBaseConfig()
        {
            if (ConfigReferenceService.Instance != null)
            {
                if (_hashCache == 0)
                {
                    _hashCache = Guid.GetHashCode();
                }

                var foundConfig = ConfigReferenceService.Instance.TryFindBaseConfig<T>(_hashCache);
                if (foundConfig != null)
                {
                    return foundConfig;
                }
            }

#if UNITY_EDITOR
            var assetPath = UnityEditor.AssetDatabase.GUIDToAssetPath(Guid);
            var editorAsset = UnityEditor.AssetDatabase.LoadAssetAtPath<T>(assetPath);

            if (Application.isPlaying && editorAsset != null)
            {
                Debug.LogError($"ConfigReference for {editorAsset.name} was not found in the baseConfigService list. " +
                    $"This is okay in the editor, but not okay in builds. Please refresh the baseConfigService.", editorAsset);
            }

            return editorAsset;
#endif

#pragma warning disable CS0162 // Unreachable code detected
            Debug.LogError($"ConfigReferencec {ObjName} [{Guid}] was not found?");
            return null;
#pragma warning restore CS0162 // Unreachable code detected
        }

        public override bool Equals(object obj)
        {
            return obj is ConfigReference<T> reference &&
                   Guid == reference.Guid;
        }
    }
}