namespace Assets.Scripts.SaveLoad
{
    /// <summary>
    /// Interface for objects that can save and load data 
    /// to persist between game sessions.
    /// 
    /// it's very similar to <see cref="IDataPersist"/> but only get call when persisting data (eg. savepoint)
    /// </summary>
    public interface ISavePersist
    {
        /// <summary>
        /// Load the state from the `GameData`.
        /// </summary>
        void LoadData(in GameData data);

        /// <summary>
        /// Save the state to the `GameData` for future `LoadData` calls.
        /// </summary>
        void SaveData(ref GameData data);
    }
}