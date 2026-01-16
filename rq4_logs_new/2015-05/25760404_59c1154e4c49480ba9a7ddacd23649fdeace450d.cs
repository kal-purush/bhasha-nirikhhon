using System.Runtime.Serialization;

namespace SeaBattle.Common.GameEvent
{
    [DataContract]
    public class GameEvent
    {
        [DataMember]
        public long TimeStamp { get; private set; }
        [DataMember]
        public EventType Type;
        [DataMember]
        public byte[] ExtraData { get; private set; }

        public GameEvent(long timeStamp, EventType type, byte[] extraData = null)
        {
            TimeStamp = timeStamp;
            Type = type;
            ExtraData = extraData;
        }
    }
}