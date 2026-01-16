
namespace BaseEngine.FSM
{
    public class FSMChangeData
    {
        private FSMStateRoot lastState;
        private object[] @params;

        /// <summary>
        /// 参数列表
        /// </summary>
        public object[] Params
        {
            get { return @params; }
        }

        /// <summary>
        /// 上一个状态机
        /// </summary>
        public FSMStateRoot LastState
        {
            get { return lastState; }
        }

        private FSMChangeData() { }


        internal static FSMChangeData Create(object[] p, FSMStateRoot l)
        {
            return new FSMChangeData()
            {
                lastState = l,
                @params = p
            };
        }
    }
}