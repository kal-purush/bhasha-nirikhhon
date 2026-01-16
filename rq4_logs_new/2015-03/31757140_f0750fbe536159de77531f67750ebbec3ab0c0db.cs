using System;
using System.Collections.Generic;
using System.Text;
namespace BaseEngine
{
    public delegate void BehaviorTreeEventHandler(BehaviorControl sender);
    /// <summary>
    /// 行为控制
    /// </summary>
    public sealed class BehaviorControl : MetaScriptableHWQ
    {
        internal static List<BehaviorControl> BCList = new List<BehaviorControl>();
        public event BehaviorTreeEventHandler OnRestartEvent;
        public event BehaviorTreeEventHandler OnCompleteEvent;
        private readonly Blackboard blackboard = new Blackboard();

        private BehaviorControl()
        {
            BCList.Add(this);
        }



        private bool paused = false;
        public bool Paused
        {
            get { return paused; }
            set { paused = true; }
        }

        private bool isRunning = false;
        public bool IsRunning
        {
            get { return isRunning; }
        }

        private bool isRestartOnComplete = false;
        public bool IsRestartOnComplete
        {
            get { return isRestartOnComplete; }
            set { isRestartOnComplete = value; }
        }

        private bool isComplete = false;
        public bool IsComplete
        {
            get { return isComplete; }
        }

        public Blackboard BlackBoard
        {
            get
            {
                return blackboard;
            }
        }
        private RootNode rootNode = null;

        public RootNode RootNode
        {
            get { return rootNode; }
        }

        public RootNode ClearRootNode()
        {
            var result = rootNode;
            rootNode = null;
            return result;
        }

        protected void Complete(TaskState state)
        {

            if (OnCompleteEvent != null)
                OnCompleteEvent(this);

            if (isRestartOnComplete)
            {
                Restart();
            }
        }

        protected void Restart()
        {
            isComplete = false;

            if (OnRestartEvent != null)
                OnRestartEvent(this);

        }


        public void Run()
        {
            if (isRunning)
                return;
            isRunning = true;
        }

        public void Pause()
        {
            if (!isRunning || !paused)
                return;
            paused = true;
        }

        public void Resume()
        {
            if (!isRunning || !paused)
                return;
            paused = false;
        }

        public TaskState Update()
        {
            if (!isRunning || rootNode == null)
                return TaskState.Failure;
            if (paused)
                return TaskState.Inactive;
            if (isComplete)
                return TaskState.Success;

            TaskState result = rootNode.OnTick();
            isComplete = (result == TaskState.Success || result == TaskState.Failure);
            if (isComplete)
            {
                Complete(result);
            }
            return result;
        }

        public static BehaviorControl Create()
        {
            return CreateInstance<BehaviorControl>();
        }
    }
}