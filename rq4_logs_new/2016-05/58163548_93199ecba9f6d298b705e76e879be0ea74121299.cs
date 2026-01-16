using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;

namespace Core
{
    [DataContract]
    public class Game
    {
        [DataMember]
        public uint Id { get;  set; }
        [DataMember]
        public Field CurrentField { get;  set; }
        [DataMember]
        public bool IsGame;
        [DataMember]
        public GameSettings Settings { get; set; }

        public Game()
        {

        }
        public Game(GameSettings sett = null)
        {
            if (sett == null)
            {
                Settings = new GameSettings();
                Settings.SetStandartPreset();
            }
            else
                Settings = sett;
            CurrentField = new Field(Settings.Size.Key, Settings.Size.Value);
            foreach (var item in Settings.StartPreset)
            {
                CurrentField.AddCell(new Grass(item.Key, item.Value));
            }

            Id = (uint)GameManager.Games.Count;
            IsGame = true;
        }

        public void DeleteCell(int posX, int posY)
        {
            CurrentField.GameObjects.Remove(CurrentField.Cells[posX][posY]);
            CurrentField.Cells[posX][posY] = null;
        }
    }
}