namespace TowerDefense.Interfaces
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;

    public interface IMonsterWave : IMonsterCreator, INotifyPropertyChanged
    {
        IEnumerable<IMonsterGroup> Groups
        {
            get;
        }

        TimeSpan SpawnTime
        {
            get;
        }

        bool Finished
        {
            get;
        }

        int Level
        {
            get;
        }

        void NextLevel();
    }
}