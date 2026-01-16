namespace Sparkade.SparkTools.CustomProjectSettings.Editor
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using UnityEditor;
    using UnityEngine;

    /// <summary>
    /// Editor-only settings manager capable of creating, destroying, writing, and reading settings.
    /// </summary>
    public static class EditorSettingsManager
    {
        private static readonly Dictionary<Type, SettingsAsset> SettingsCache = new Dictionary<Type, SettingsAsset>();

        /// <summary>
        /// Gets the absolute path to where custom project settings are stored.
        /// </summary>
        public static string SettingsPath => Path.Combine(Directory.GetParent(Application.dataPath).FullName, "ProjectSettings", "CustomSettings");

        /// <summary>
        /// Creates a cusom project setting of type 'T'.
        /// </summary>
        /// <typeparam name="T">Type of SettingsAsset to be created.</typeparam>
        public static void CreateSettings<T>()
            where T : SettingsAsset
        {
            if (SettingsExists<T>())
            {
                Debug.LogError($"SettingsAsset '{SettingsManager.GetSettingsName<T>()}' already exists.");
                return;
            }

            T asset = ScriptableObject.CreateInstance<T>();
            asset.Reset();

            if (!Directory.Exists(SettingsPath))
            {
                Directory.CreateDirectory(SettingsPath);
            }

            File.WriteAllText(Path.Combine(SettingsPath, GetSettingsFilename<T>()), JsonUtility.ToJson(asset));
        }

        /// <summary>
        /// Destroys a cusom project setting of type 'T'.
        /// </summary>
        /// <typeparam name="T">Type of SettingsAsset to be destroyed.</typeparam>
        public static void DestroySettings<T>()
            where T : SettingsAsset
        {
            if (!SettingsExists<T>())
            {
                Debug.LogError($"SettingsAsset '{SettingsManager.GetSettingsName<T>()}' does not exist.");
                return;
            }

            File.Delete(Path.Combine(SettingsPath, GetSettingsFilename<T>()));
            SettingsCache.Remove(typeof(T));
        }

        /// <summary>
        /// Returns a custom project setting of type 'T'.
        /// </summary>
        /// <typeparam name="T">Type of SettingsAsset to be returned.</typeparam>
        /// <returns>The SettingsAsset of type 'T'. If it does not exist, null is returned.</returns>
        public static T LoadSettings<T>()
            where T : SettingsAsset
        {
            return GetCachedSettings<T>();
        }

        /// <summary>
        /// Gets whether a custom project setting of type 'T' exists.
        /// </summary>
        /// <typeparam name="T">The type of SettingsAsset you are checking for.</typeparam>
        /// <returns>True if the custom project setting of type 'T' exists, otherwise false.</returns>
        public static bool SettingsExists<T>()
            where T : SettingsAsset
        {
            return LoadSettings<T>() != null;
        }

        /// <summary>
        /// Opens the SettingsAsset of type 'T' in the inspector.
        /// </summary>
        /// <typeparam name="T">The type of SettingsAsset to inspect.</typeparam>
        public static void InspectSettings<T>()
            where T : SettingsAsset
        {
            T asset = GetCachedSettings<T>();

            if (asset == null)
            {
                Debug.LogError($"SettingsAsset '{SettingsManager.GetSettingsName<T>()}' does not exist.");
                return;
            }

            Selection.activeObject = asset;
        }

        /// <summary>
        /// Loads a custom project setting, and creates it if it did not exist.
        /// </summary>
        /// <typeparam name="T">Type of SettingsAsset to be returned.</typeparam>
        /// <returns>The SettingsAsset of type 'T'.</returns>
        public static T LoadOrCreateSettings<T>()
            where T : SettingsAsset
        {
            if (!SettingsExists<T>())
            {
                CreateSettings<T>();
            }

            return LoadSettings<T>();
        }

        /// <summary>
        /// Inspects a custom project setting, and creates it if it did not exist.
        /// </summary>
        /// <typeparam name="T">The type of SettingsAsset to inspect.</typeparam>
        public static void InspectOrCreateSettings<T>()
            where T : SettingsAsset
        {
            if (!SettingsExists<T>())
            {
                CreateSettings<T>();
            }

            InspectSettings<T>();
        }

        /// <summary>
        /// Manually saves all currently cached settings.
        /// </summary>
        public static void SaveCachedSettings()
        {
            foreach (KeyValuePair<Type, SettingsAsset> pair in SettingsCache)
            {
                File.WriteAllText(Path.Combine(SettingsPath, GetSettingsFilename(pair.Key)), JsonUtility.ToJson(pair.Value));
            }

            SettingsCache.Clear();
        }

        private static T GetCachedSettings<T>()
            where T : SettingsAsset
        {
            if (!SettingsCache.ContainsKey(typeof(T)))
            {
                string filePath = Path.Combine(SettingsPath, GetSettingsFilename<T>());

                if (!File.Exists(filePath))
                {
                    return null;
                }

                SettingsCache[typeof(T)] = ScriptableObject.CreateInstance<T>();
                JsonUtility.FromJsonOverwrite(File.ReadAllText(filePath), SettingsCache[typeof(T)]);
            }

            return SettingsCache[typeof(T)] as T;
        }

        private static string GetSettingsFilename(Type type)
        {
            return Path.ChangeExtension(SettingsManager.GetSettingsName(type), "asset");
        }

        private static string GetSettingsFilename<T>()
        {
            return GetSettingsFilename(typeof(T));
        }
    }
}