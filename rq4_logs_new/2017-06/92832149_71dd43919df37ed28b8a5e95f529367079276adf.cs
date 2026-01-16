using System.Collections.Generic;
using System.Linq;

namespace AppmetrCS.Actions
{
    #region using directives

    using System;
    using System.Runtime.Serialization;

    #endregion

    [DataContract]
    public class TrackState : AppMetrAction
    {
        private const String ACTION = "trackState";

        [DataMember(Name = "state")]
        public Dictionary<String, Object> State { get; set; } = new Dictionary<String, Object>();

        public TrackState() : base(ACTION)
        {
        }

        public TrackState(Dictionary<String, Object> state) : base(ACTION)
        {
            State = state;
        }

        public override Int32 CalcApproximateSize()
        {
            return base.CalcApproximateSize() + 4;
        }
             
        public override String ToString()
        {
            return $"{base.ToString()},state={{{String.Join(",", State.Select(kv => kv.Key + "=" + kv.Value).ToArray())}}}";
        }
    }
}