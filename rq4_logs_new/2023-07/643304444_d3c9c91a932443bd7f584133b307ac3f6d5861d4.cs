using UnityEngine;

namespace KamranWali.CodeOptPro.ScriptableObjects
{
    [CreateAssetMenu(fileName = "VariablePath",
                     menuName = "CodeOptPro/ScriptableObjects/Managers/" +
                                "VariablePath",
                     order = 1)]
    public class VariablePath : BaseScriptableObject
    {
        [SerializeField] private string[] _paths;

        /// <summary>
        /// This method sets the path of the index.
        /// </summary>
        /// <param name="index">The indexth path to set, of type int</param>
        /// <param name="path">The path to set, of type string</param>
        public void SetPath(int index, string path) => _paths[index] = path;

        /// <summary>
        /// This method gets the indexth path.
        /// </summary>
        /// <param name="index">The indexth path to get, of type int</param>
        /// <returns>The indexth path, of type int</returns>
        public string GetPath(int index) => _paths[index];

        /// <summary>
        /// Number of paths saved.
        /// </summary>
        /// <returns>The number of path saved, of type int</returns>
        public int Size() => _paths.Length;

        protected override string GetToStringValues()
        {
            throw new System.NotImplementedException();
        }
    }
}