using Econocom.DataGenerator.Entities.Interfaces;

namespace Econocom.DataGenerator.Entities.Entities
{
    public class PanicData: IData
    {
        public string DeviceId { get; set; }
        public string Type { get; set; }
        public IPayload Payload { get; set; }
    }
}