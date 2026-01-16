using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BaseEngine.Bridge
{

    public sealed class BridgeSender
    {
        private BridgeSender() { }
        private object[] sendList;
        private object send;
        public object[] parList{
            internal set{
                sendList = value;
            }
            get{
                return sendList;
            }
        }

        public object Sender
        {
            internal set
            {
                send = value;
            }
            get
            {
                return send;
            }
        }

        internal static BridgeSender Create()
        {
            return new BridgeSender();
        }
    }


    /// <summary>
    /// 会议控制
    /// </summary>
    public sealed class BridgeControl : MetaScriptableHWQ
    {
        private List<BridgeNode>  members = new List<BridgeNode>();
        private List<BridgeNode> noJobList = new List<BridgeNode>();
        private Dictionary<string, List<BridgeNode>> memberDic = new Dictionary<string, List<BridgeNode>>();

        private BridgeControl() { }


        /// <summary>
        /// 添加会议成员
        /// </summary>
        public void AddMember(BridgeNode bn)
        {
            if (bn == null)
                return;
            bn.SetControl(this);
            if (!members.Contains(bn))
                members.Add(bn);
            if (!noJobList.Contains(bn))
                noJobList.Add(bn);
        }

        internal void DistributionJob(string job, BridgeNode bn)
        {
            if (memberDic.ContainsKey(job))
            {
                memberDic[job].Add(bn);
            }
            else
            {
                memberDic.Add(job, new List<BridgeNode>());
                memberDic[job].Add(bn);
            }
            noJobList.Remove(bn);
        }

        internal void ExitBridge(BridgeNode bn)
        {
            members.Remove(bn);
            if (memberDic.ContainsKey(bn.jobName))
            {
                memberDic[bn.jobName].Remove(bn);
            }

        }

        /// <summary>
        /// 部门通知
        /// </summary>
        /// <param name="nodeName"></param>
        /// <param name="noticeName"></param>
        /// <param name="objList"></param>
        internal void NoticeByJob(string job ,string noticeName, BridgeSender bs)
        {
            if (string.IsNullOrEmpty(job))
            {
                foreach (BridgeNode bn in noJobList)
                {
                    if (bn)
                        bn.ExCommang(noticeName, bs);
                }
            }
        }

    }
}