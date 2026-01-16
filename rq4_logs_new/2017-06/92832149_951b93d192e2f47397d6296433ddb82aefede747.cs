namespace AppmetrCS.Actions
{
    #region using directives

    using System;
    using System.Runtime.Serialization;

    #endregion

    [DataContract]
    public class TrackIdentify : AppMetrAction
    {
        private const String ACTION = "identify";

        protected TrackIdentify() : base(ACTION)
        {
        }

        public TrackIdentify(String userId) : base(ACTION)
        {
            UserId = userId;
        }

        public override Int32 CalcApproximateSize()
        {
            return base.CalcApproximateSize() + 4;
        }
    }
}