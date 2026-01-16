using BetterCoding.MessagePubSubCenter.Entity.Enums;

namespace BetterCoding.MessagePubSubCenter.Services
{
    public interface IIntervalConverter 
    {
        int Convert(IntervalByMilisecond value);
        int[] Convert(params IntervalByMilisecond[] values);
    }

    public class IntervalConverter: IIntervalConverter
    {
        public int Convert(IntervalByMilisecond value) 
        {
            return System.Convert.ToInt32(value);
        }

        public int[] Convert(params IntervalByMilisecond[] values) 
        {
           var array = values.Select(Convert).ToArray();
            return array;
        }
    }
}