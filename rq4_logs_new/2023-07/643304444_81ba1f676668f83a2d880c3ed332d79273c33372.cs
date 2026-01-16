namespace KamranWali.CodeOptPro.Managers
{
    public interface IUpdateManager
    {
        /// <summary>
        /// Gets the time delta value for the manager.
        /// </summary>
        /// <returns>The time delta value, of type float</returns>
        public float GetTimeDelta();

        /// <summary>
        /// Gets the calculated Time.deltaTime value from the manager.
        /// </summary>
        /// <returns>The calculated Time.deltaTime value, of type float</returns>
        public float GetTime();

        /// <summary>
        /// This method adds an object to the list, ONLY CALL FROM EDITOR SCRIPT.
        /// </summary>
        /// <param name="obj">The object to add, of type MonoAdvUpdate</param>
        public void AddObject(MonoAdvUpdate obj);

        /// <summary>
        /// This method removes all object from the list.
        /// </summary>
        public void ResetData();
    }
}