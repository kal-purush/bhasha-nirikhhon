using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Domain
{
    [DataContract]
    public class Games
    {
        public Games()
        {
            this.GamesPlayers = new ObservableCollection<Domain.GamesPlayers>();
        }

        [DataMember]
        public int idGame { get; set; }
        [DataMember]
        public System.DateTime gameDate { get; set; }
        [DataMember] 
        public System.TimeSpan gameTime { get; set; }
        [DataMember]
        public ObservableCollection<Domain.GamesPlayers> GamesPlayers { set; get; }
    }
}