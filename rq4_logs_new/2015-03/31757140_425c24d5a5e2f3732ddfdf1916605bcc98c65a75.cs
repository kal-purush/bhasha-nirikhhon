using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BaseEngine.FSM
{
    /// <summary>
    /// a
    /// </summary>
    public class FSMStateImpl : FSMState
    {
        private FSMStateImpl() { }

        /// <summary>
        /// 创建状态机
        /// </summary>
        /// <param name="e">enter方法</param>
        /// <param name="o">OnUpdate方法</param>
        /// <param name="x">Exit方法</param>
        /// <returns></returns>
        public static FSMStateImpl Create(Action e,Action o ,Action x)
        {
            FSMStateImpl fsm = new FSMStateImpl();
            fsm.Init(e, o, x);
            return fsm;
        }

    }
}