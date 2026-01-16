using KamranWali.CodeOptPro.ScriptableObjects.FixedVars;
using UnityEditor;
using UnityEngine;

namespace KamranWali.CodeOptPro.Editor
{
    public abstract class BaseCodeOptPro : EditorWindow
    {
        [SerializeField] protected FixedStringVar version;

        private string _log;
        #region Logo Fields
        private bool _isSetLogo;
        private Vector2 _scrollPos;
        private Texture _texLogo;
        private Texture _texLogoName;
        private readonly string _logoPath = "KamranWali/CodeOptPro/Images/CodeOptProLogo_Only_500x651";
        private readonly string _logoNamePath = "KamranWali/CodeOptPro/Images/CodeOptProLogo_Name_500x89";
        private GUIStyle _versionStyle;
        private readonly int _fontSize = 18;
        #endregion

        private void OnGUI()
        {
            if (!_isSetLogo) // Condition to set logo
            {
                _texLogo = Resources.Load<Texture>(_logoPath);
                _texLogoName = Resources.Load<Texture>(_logoNamePath);
                _versionStyle = new GUIStyle();
                _versionStyle.fontSize = _fontSize;
                _versionStyle.normal.textColor = Color.white;
                _isSetLogo = true;
            }

            _scrollPos = EditorGUILayout.BeginScrollView(_scrollPos);
            InitInput();

            EditorGUI.BeginDisabledGroup(true);
            _log = EditorGUILayout.TextArea(_log);
            EditorGUI.EndDisabledGroup();

            if (_isSetLogo) // Condition to show the logo
            {
                GUILayout.Space(30f);
                GUILayout.Box(_texLogo, new GUILayoutOption[] { GUILayout.Width(100f), GUILayout.Height(130.2f), GUILayout.ExpandWidth(true), GUILayout.ExpandHeight(false) });
                GUILayout.Box(_texLogoName, new GUILayoutOption[] { GUILayout.Width(200f), GUILayout.Height(35.6f), GUILayout.ExpandWidth(true), GUILayout.ExpandHeight(false) });
                GUILayout.Space(10f);
                GUILayout.BeginHorizontal();
                GUILayout.Space(5f);
                EditorGUILayout.LabelField(version.GetValue(), _versionStyle);
                GUILayout.EndHorizontal();
            }

            EditorGUILayout.EndScrollView();
        }

        /// <summary>
        /// This method sets the log message.
        /// </summary>
        /// <param name="msg">The log message to set, of type string</param>
        protected void SetLog(string msg) => _log = msg;

        /// <summary>
        /// This method writes to log.
        /// </summary>
        /// <param name="msg">The message to write, of type string</param>
        protected void WriteToLog(string msg) => _log += $"\n{msg}";

        /// <summary>
        /// This method initializes inputs and is called from OnGUI() method.
        /// </summary>
        protected abstract void InitInput();
    }
}