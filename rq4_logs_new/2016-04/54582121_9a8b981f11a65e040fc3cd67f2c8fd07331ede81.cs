
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace RootNav.Core.MixtureModels
{
	public delegate void TaskThreadProgressChangedHandler(int progress);

	public delegate void EMManagerThreadProgressCompleted();

	class EMManagerThread
    {
        public class EMWorkNotCompletedException : Exception
        {
            public EMWorkNotCompletedException(String message)
                : base(message)
            {
            }

            public EMWorkNotCompletedException(String message, Exception innerException)
                : base(message, innerException)
            {
            }
        }

        private delegate void DoWorkDelegate(byte[] intensityBuffer, int bufferWidth, int bufferHeight, int patchSize, int threads);
        public event ProgressChangedEventHandler ProgressChanged;
        public event RunWorkerCompletedEventHandler ProgressCompleted;

        public Dictionary<EMPatch,GaussianMixtureModel> Mixtures = null;
        public int Width { get; set; }
        public int Height { get; set; }
        public EMConfiguration Configuration { get; set; }
        public int ThreadCount { get; set; }
        public byte[] IntensityBuffer { get; set; }
        private volatile bool workCompleted = false;

        public bool IsWorkCompleted
        {
            get
            {
                return this.workCompleted;
            }
        }

        // Worker information
        //private List<EMWorker> currentWorkers = null;
		private List<EMThread> currentWorkers = null;

        private List<bool> currentWorkersCompleted = null;
        private List<int> currentWorkersProgress = null;
        private int currentProgress;

        public void Run()
        {
			Console.WriteLine ("Run with " + ThreadCount.ToString() + " threads.");

            // Create thread workers
			currentWorkers = new List<EMThread>();
            currentWorkersCompleted = new List<bool>();
            currentWorkersProgress = new List<int>();
            currentProgress = 0;
            for (int worker = 0; worker < ThreadCount; worker++)
            {
				EMThread thread = new EMThread();

                thread.ProgressChanged += new ProgressChangedEventHandler(OnWorkerProgressChanged);
				thread.ProgressCompleted += new RunWorkerCompletedEventHandler(OnWorkerProgressCompleted);
                currentWorkers.Add(thread);
                currentWorkersCompleted.Add(false);
                currentWorkersProgress.Add(0);
            }

            // Create data objects for workers
            List<List<EMPatch>> workerPatches = new List<List<EMPatch>>();
            for (int t = 0; t < ThreadCount; t++)
                workerPatches.Add(new List<EMPatch>());
            
            // Patch size
            int patchArrayWidth = (int)Math.Ceiling(Width / (double)Configuration.PatchSize);
            int patchArrayHeight = (int)Math.Ceiling(Height / (double)Configuration.PatchSize);

            int currentThreadIndex = 0;
            for (int patchX = 0; patchX < patchArrayWidth; patchX++)
            {
                for (int patchY = 0; patchY < patchArrayHeight; patchY++)
                {
                    // Calculate bounding box of the patch
                    int left = patchX * Configuration.PatchSize;
                    int top = patchY * Configuration.PatchSize;
                    int right = Math.Min(left + Configuration.PatchSize, Width);
                    int bottom = Math.Min(top + Configuration.PatchSize, Height);

                    // Create a new EMPatch
                    EMPatch patch = new EMPatch(patchX, patchY, left, right, top, bottom);

                    // Assign patch to a worker thread
                    workerPatches[currentThreadIndex].Add(patch);
                    currentThreadIndex = (currentThreadIndex + 1) % ThreadCount;
                }
            }
            
            // Launch workers
            for (int worker = 0; worker < currentWorkers.Count; worker++)
            {
                currentWorkers[worker].IntensityBuffer = IntensityBuffer;
                currentWorkers[worker].Patches = workerPatches[worker];
                currentWorkers[worker].Width = this.Width;
                currentWorkers[worker].Height = this.Height;
                currentWorkers[worker].Configuration = this.Configuration;
				currentWorkers[worker].Start();

				//Console.WriteLine ("Lauching...");
            }
        }

        private void OnWorkerProgressChanged(object sender, ProgressChangedEventArgs args)
        {
			Console.WriteLine ("OnWorkerProgressChanged of EMManager...");

			EMThread worker = sender as EMThread;

            if (worker == null)
            {
                return;
            }

            lock (currentWorkersProgress)
            {
                currentWorkersProgress[currentWorkers.IndexOf(worker)] = args.ProgressPercentage;
                int combinedProgress = 0;
                combinedProgress = currentWorkersProgress.Sum() / currentWorkersProgress.Count;
           

                if (currentProgress != combinedProgress)
                {
                    currentProgress = combinedProgress;
                    if (this.ProgressChanged != null)
                        this.ProgressChanged(this, new ProgressChangedEventArgs(currentProgress, null));
                }
            }
        }

        private void OnWorkerProgressCompleted(object sender, RunWorkerCompletedEventArgs args)
        {
			EMThread worker = sender as EMThread;
			Console.WriteLine("OnWorkerProgressCompleted of EMManager");

            if (worker == null)
            {
                return;  // TODO: Handle Error
            }

            if (args.Error != null)
            {
                // TODO: Handle Error
				Console.WriteLine("An error occurred in an EM Worker");
                throw new EMWorkNotCompletedException("An error occurred in an EM Worker", args.Error);
            }

            this.currentWorkersCompleted[currentWorkers.IndexOf(worker)] = true;

            if (!this.currentWorkersCompleted.Contains(false))
            {
                this.Mixtures = new Dictionary<EMPatch, GaussianMixtureModel>();
				foreach (EMThread w in this.currentWorkers)
                {
                    foreach (Tuple<EMPatch, GaussianMixtureModel> t in w.Mixtures)
                    {
                        this.Mixtures.Add(t.Item1, t.Item2);
                    }
                }

                this.workCompleted = true;

                if (this.ProgressCompleted != null)
                    this.ProgressCompleted(this, new RunWorkerCompletedEventArgs(null, null, false));
            }
        }

        public int[] PatchHistogramDataFromPoint(System.Windows.Point imagePoint)
        {
            if (!workCompleted)
                throw new EMWorkNotCompletedException("Cannot obtain intensity data before E-M work has been completed");

            int patchX = (int)imagePoint.X / this.Configuration.PatchSize;
            int patchY = (int)imagePoint.Y / this.Configuration.PatchSize;
            int left = patchX * this.Configuration.PatchSize;
            int top = patchY * this.Configuration.PatchSize;
            int right = Math.Min(left + this.Configuration.PatchSize, this.Width);
            int bottom = Math.Min(top + this.Configuration.PatchSize, this.Height);
            
            // Obtain mixture model for current patch
            EMPatch key = new EMPatch(patchX, patchY, left, right, top, bottom);

            // Obtain intensity data for current patch
            int[] histogramData = key.CreateHistogram(this.IntensityBuffer, this.Width, this.Height);

            return histogramData;
        }

        public EMPatch PatchFromPoint(System.Windows.Point imagePoint)
        {
            if (!workCompleted)
                throw new EMWorkNotCompletedException("Cannot obtain patch data before E-M work has been completed");

            int patchX = (int)imagePoint.X / this.Configuration.PatchSize;
            int patchY = (int)imagePoint.Y / this.Configuration.PatchSize;
            int left = patchX * this.Configuration.PatchSize;
            int top = patchY * this.Configuration.PatchSize;
            int right = Math.Min(left + this.Configuration.PatchSize, this.Width);
            int bottom = Math.Min(top + this.Configuration.PatchSize, this.Height);

            // Obtain mixture model for current patch
            return new EMPatch(patchX, patchY, left, right, top, bottom);
        }


        public GaussianMixtureModel PatchGaussianMixtureModelFromPoint(System.Windows.Point imagePoint)
        {
            if (!workCompleted)
                throw new EMWorkNotCompletedException("Cannot obtain mixture data before E-M work has been completed");

            int patchX = (int)imagePoint.X / this.Configuration.PatchSize;
            int patchY = (int)imagePoint.Y / this.Configuration.PatchSize;
            int left = patchX * this.Configuration.PatchSize;
            int top = patchY * this.Configuration.PatchSize;
            int right = Math.Min(left + this.Configuration.PatchSize, this.Width);
            int bottom = Math.Min(top + this.Configuration.PatchSize, this.Height);

            // Obtain mixture model for current patch
            EMPatch key = new EMPatch(patchX, patchY, left, right, top, bottom);

            return this.Mixtures[key];
        }

    } 
}