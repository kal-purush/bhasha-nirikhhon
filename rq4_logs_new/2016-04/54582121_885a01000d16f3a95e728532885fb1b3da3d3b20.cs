using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
//using System.Windows.Media.Imaging;
using System.Windows;
using System.ComponentModel;
using System.Threading;

namespace RootNav.Core.LiveWires
{
    public class LiveWireThread
    {
		public event ProgressChangedEventHandler ProgressChanged;
		public event RunWorkerCompletedEventHandler ProgressCompleted;

		protected Thread actualThread;

        public enum WorkMode
        {
            Work,
            ReWork
        }

        public WorkMode Mode { get; set; }
		public LiveWireThread()
		{
			actualThread = new Thread (new ThreadStart (OnDoWork));
		}

		protected virtual void OnProgressChanged(ProgressChangedEventArgs e)
        {
            //base.OnProgressChanged(e);
			if (this.ProgressChanged != null) {
				this.ProgressChanged (this, e);
			}
        }
		protected virtual void  OnDoWork ()
		{
			//subclass should implement this function
			if (this.ProgressCompleted != null) 
			{
				this.ProgressCompleted(this, new RunWorkerCompletedEventArgs(null, null, false));
			}
		}

		public virtual void Run()
		{
			actualThread.Start();
		}
			
    }

    public class LiveWirePrimaryThread : LiveWireThread
    {
        public Dictionary<Tuple<int, int>, List<Point>> Paths { get; set; }
        public Dictionary<Tuple<int, int>, List<int>> ControlPointIndices { get; set; }

        // DoWork
        public List<Tuple<int, int>> TerminalPairs { get; set; }
        public RootTerminalCollection TerminalCollection { get; set; }
        public LiveWireGraph Graph { get; set; }
        public double[,] DistanceMap { get; set; }

        // ReWork
        public List<LiveWirePrimaryPath> ReWorkPaths { get; set; }

        protected override void  OnDoWork()
        {
            if (this.Paths == null)
            {
                this.Paths = new Dictionary<Tuple<int, int>, List<Point>>();
            }

            if (this.ControlPointIndices == null)
            {
                this.ControlPointIndices = new Dictionary<Tuple<int, int>, List<int>>();
            }

            if (this.Mode == WorkMode.Work)
            {
                int current = 0;
                int total = TerminalPairs.Count;
                foreach (Tuple<int, int> terminalPair in TerminalPairs)
                {
                    List<int> indices = null;
                    List<Point> path = null;
                    TargetPathPoint unusedPoint;
                    LiveWireSegmentation.DijkstraBetweenPoints(Graph, DistanceMap, out path, out indices, out unusedPoint, null, TerminalCollection[terminalPair.Item1].Position, TerminalCollection[terminalPair.Item2].Position);
                    this.Paths.Add(terminalPair, path);
                    this.ControlPointIndices.Add(terminalPair, indices);

                    // Signal progress changed
                    base.OnProgressChanged(new ProgressChangedEventArgs((int)(++current * 100.0 / total), null));
                }
            }
            else if (this.Mode == WorkMode.ReWork)
            {
                int current = 0;
                int total = ReWorkPaths.Count;
                for (int i = 0; i < ReWorkPaths.Count; i++)
                {
                    LiveWirePrimaryPath currentPath = ReWorkPaths[i];

                    List<Point> pathKeyPoints = new List<Point>();
                    pathKeyPoints.Add(TerminalCollection[currentPath.SourceIndex].Position);
                    foreach (Point p in currentPath.IntermediatePoints)
                        pathKeyPoints.Add(p);

                    pathKeyPoints.Add(TerminalCollection[currentPath.TipIndex].Position);

                    List<Point> newPoints = null;
                    List<int> newIndices = null;
                    TargetPathPoint unusedPoint;

                    LiveWireSegmentation.DijkstraBetweenPoints(Graph, DistanceMap, out newPoints, out newIndices, out unusedPoint, null, pathKeyPoints.ToArray());

                    currentPath.Path = newPoints;
                    currentPath.Indices = newIndices;

                    // Signal progress changed
                    base.OnProgressChanged(new ProgressChangedEventArgs((int)(++current * 100.0 / total), null));
                }
            }
        }
    }

    public class LiveWireLateralThread : LiveWireThread
    {
        public Dictionary<int, List<Point>> Paths { get; set; }
        public Dictionary<int, List<int>> ControlPointIndices { get; set; }
        public Dictionary<int, TargetPathPoint> TargetPathIndices { get; set; }

        // DoWork
        public List<int> LateralTerminals { get; set; }
        public RootTerminalCollection TerminalCollection { get; set; }
        public LiveWireGraph Graph { get; set; }
        public Dictionary<Int32Point, int> CurrentPathIndexes { get; set; }
        public double[,] DistanceMap { get; set; }

        // ReWork
        public List<LiveWireLateralPath> ReWorkPaths { get; set; }

        protected override void OnDoWork()
        {
            if (this.Paths == null)
            {
                this.Paths = new Dictionary<int, List<Point>>();
            }

            if (this.ControlPointIndices == null)
            {
                this.ControlPointIndices = new Dictionary<int, List<int>>();
            }

            if (this.TargetPathIndices == null)
            {
                this.TargetPathIndices = new Dictionary<int, TargetPathPoint>();
            }

            if (this.Mode == WorkMode.Work)
            {
                int current = 0;
                int total = LateralTerminals.Count;
                foreach (int lateral in LateralTerminals)
                {
                    List<int> indices;
                    List<Point> path;
                    TargetPathPoint pathTarget;
                    LiveWireSegmentation.DijkstraBetweenPoints(Graph, DistanceMap, out path, out indices, out pathTarget, CurrentPathIndexes, TerminalCollection[lateral].Position);
                    this.Paths.Add(lateral, path);
                    this.ControlPointIndices.Add(lateral, indices);
                    this.TargetPathIndices.Add(lateral, pathTarget);

                    // Signal progress changed
                    base.OnProgressChanged(new ProgressChangedEventArgs((int)(++current * 100.0 / total), null));
                }
            }
            else if (this.Mode == WorkMode.ReWork)
            {
                int current = 0;
                int total = ReWorkPaths.Count;
                for (int i = 0; i < ReWorkPaths.Count; i++)
                {
                    LiveWireLateralPath currentPath = ReWorkPaths[i];

                    List<int> indices;
                    List<Point> path;
                    TargetPathPoint pathTarget;

                    List<Point> pathKeyPoints = new List<Point>();
                    pathKeyPoints.Add(TerminalCollection[currentPath.SourceIndex].Position);
                    foreach (Point p in currentPath.IntermediatePoints)
                        pathKeyPoints.Add(p);

                    LiveWireSegmentation.DijkstraBetweenPoints(Graph, DistanceMap, out path, out indices, out pathTarget, CurrentPathIndexes, pathKeyPoints.ToArray());

                    currentPath.Path = path;
                    currentPath.Indices = indices;
                    currentPath.TargetPoint = pathTarget;

                    // Signal progress changed
                    base.OnProgressChanged(new ProgressChangedEventArgs((int)(++current * 100.0 / total), null));
                }
            }
        }
    }
}