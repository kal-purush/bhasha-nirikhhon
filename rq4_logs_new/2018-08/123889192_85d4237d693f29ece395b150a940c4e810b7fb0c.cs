using System;
using UnityEngine;

namespace UnityModule.Settings
{
    public class BuildAssetBundleSetting : Setting<BuildAssetBundleSetting>, IEnvironmentSetting
    {
        private const string EnvironmentVariableKeyForceUploadAssetBundle = "FORCE_UPLOAD_ASSETBUNDLE";

        [SerializeField] private bool forceUploadAssetBundle = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(EnvironmentVariableKeyForceUploadAssetBundle));

        public bool ForceUploadAssetBundle => forceUploadAssetBundle;
    }
}