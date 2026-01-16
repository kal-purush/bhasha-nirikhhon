namespace Meridian.MeaningfulToString.Tests.Model
{
    public abstract class DatabaseEntity<TIdType> : MeaningfulBase
        where TIdType : struct
    {
        public TIdType Id
        {
            get;
            set;
        }
    }
}