using System.IO;
using UnityEditor;
using UnityEngine;
using WelwiseGamesSDK.Internal;
using WelwiseGamesSDK.Shared;

namespace WelwiseGames.Editor
{
    public class SDKSettingsEditor : EditorWindow
    {
        private SDKSettings _settings;
        private SerializedObject _serializedSettings;
        private SerializedProperty _supportedSDKType;
        private SerializedProperty _playerId;
        private SerializedProperty _debugDeviceType;
        private SerializedProperty _debugLanguageCode;
        private SupportedSDKType _lastSDKType;

        [MenuItem("Tools/WelwiseGamesSDK/SDK Settings")]
        public static void ShowWindow()
        {
            GetWindow<SDKSettingsEditor>("SDK Settings");
        }

        void OnEnable()
        {
            _settings = SDKSettings.LoadOrCreateSettings();
            _serializedSettings = new SerializedObject(_settings);

            _supportedSDKType = _serializedSettings.FindProperty("_supportedSDKType");
            _playerId = _serializedSettings.FindProperty("_playerId");
            _debugDeviceType = _serializedSettings.FindProperty("_debugDeviceType");
            _debugLanguageCode = _serializedSettings.FindProperty("_debugLanguageCode");
            _lastSDKType = _settings.SupportedSDKType;
        }

        void OnGUI()
        {
            _serializedSettings.Update();
            var previousType = (SupportedSDKType)_supportedSDKType.enumValueIndex;
            EditorGUILayout.Space(10);
            EditorGUILayout.LabelField("SDK Configuration", EditorStyles.boldLabel);
            EditorGUILayout.Space(5);

            EditorGUILayout.PropertyField(_supportedSDKType, new GUIContent("SDK Type"));
            EditorGUILayout.PropertyField(_playerId, new GUIContent("Debug Player ID"));
            EditorGUILayout.PropertyField(_debugDeviceType, new GUIContent("Device Type"));
            EditorGUILayout.PropertyField(_debugLanguageCode, new GUIContent("Language Code"));

            EditorGUILayout.Space(15);

            if (GUILayout.Button("Save Settings", GUILayout.Height(30)))
            {
                SaveChanges();
            }

            _serializedSettings.ApplyModifiedProperties();
            
            if (_settings.SupportedSDKType != _lastSDKType)
            {
                HandleSDKTypeChange(_lastSDKType, _settings.SupportedSDKType);
                _lastSDKType = _settings.SupportedSDKType;
                SaveChanges();
            }
        }
        
        private void HandleSDKTypeChange(SupportedSDKType oldType, SupportedSDKType newType)
        {
            try
            {
                bool success = true;
                
                // Удаляем общие файлы
                DeleteDirectory("WebGL Templates/Welwise SDK");
                
                switch (newType)
                {
                    case SupportedSDKType.WelwiseGames:
                        success &= DeleteFile("Plugins/WebGL/yandex-games.jslib");
                        ImportPackage("welwise-games-template");
                        break;
                        
                    case SupportedSDKType.YandexGames:
                        success &= DeleteFile("Plugins/WebGL/welwise-sdk.jslib");
                        ImportPackage("yandex-games-template");
                        break;
                }

                if (success)
                {
                    UpdateWebGLTemplate();
                    AssetDatabase.Refresh();
                }
            }
            catch (System.Exception e)
            {
                Debug.LogError($"Error processing SDK change: {e.Message}");
            }
        }
        
        private bool DeleteFile(string relativePath)
        {
            string fullPath = Path.Combine(Application.dataPath, relativePath);
            string assetPath = Path.Combine("Assets", relativePath);

            if (File.Exists(fullPath))
            {
                AssetDatabase.DeleteAsset(assetPath);
                Debug.Log($"Deleted file: {assetPath}");
                return true;
            }
            return false;
        }

        private void DeleteDirectory(string relativePath)
        {
            string assetPath = Path.Combine("Assets", relativePath);
            
            if (AssetDatabase.IsValidFolder(assetPath))
            {
                FileUtil.DeleteFileOrDirectory(assetPath);
                FileUtil.DeleteFileOrDirectory(assetPath + ".meta");
                Debug.Log($"Deleted directory: {assetPath}");
            }
        }

        private void ImportPackage(string packageName)
        {
            var guids = AssetDatabase.FindAssets(packageName);
            foreach (var guid in guids)
            {
                var path = AssetDatabase.GUIDToAssetPath(guid);
                if (Path.GetFileName(path) == $"{packageName}.unitypackage")
                {
                    AssetDatabase.ImportPackage(path, false);
                    Debug.Log($"Imported {packageName} package");
                    return;
                }
            }
            Debug.LogError($"Package {packageName}.unitypackage not found!");
        }

        private void UpdateWebGLTemplate()
        {
            PlayerSettings.WebGL.template = "Welwise SDK";
            Debug.Log("WebGL template updated to 'Welwise SDK'");
        }

        private new void SaveChanges()
        {
            if (_serializedSettings != null && _settings != null)
            {
                EditorUtility.SetDirty(_settings);
                AssetDatabase.SaveAssets();
                Debug.Log("SDK settings saved successfully!");
            }
        }
    }
}