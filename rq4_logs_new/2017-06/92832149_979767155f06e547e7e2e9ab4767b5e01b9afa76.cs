namespace AppmetrCS.Actions
{
    #region using directives

    using System;
    using System.Runtime.Serialization;

    #endregion

    [DataContract]
    public class TrackLevel : AppMetrAction
    {
        private const String ACTION = "trackLevel";

        [DataMember(Name = "level")]
        public Int32 Level { get; set; }

        protected TrackLevel()
        {
        }

        public TrackLevel(Int32 level) : base(ACTION)
        {
            Level = level;
        }

        public override Int32 CalcApproximateSize()
        {
            return base.CalcApproximateSize() + 4;
        }
             
        public override String ToString()
        {
            return $"{base.ToString()},level={Level}";
        }
    }
}