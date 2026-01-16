using PollinationSDK.Api;
using System;
using System.Threading.Tasks;
using QueenbeeSDK;

namespace PollinationSDK.Wrapper
{
    public class ScheduledJobInfo
    {
        public string JobID => this.CloudJob.Id;
        public CloudJob CloudJob { get; private set; }
        public Project Project { get; private set; }
        public RecipeInterface Recipe { get; private set; }

        //[IgnoreDataMember]
        //public string Logs { get; set; }
        public ScheduledJobInfo(Project proj, string jobID): this(proj, GetJob(proj, jobID))
        {
        }

        public ScheduledJobInfo(Project proj, CloudJob run)
        {
            this.CloudJob = run;
            this.Project = proj;
            this.Recipe = GetRecipe(this.CloudJob.Status.Source);
        }

        public ScheduledJobInfo(string localRunPath)
        {
        }

        private static CloudJob GetJob(Project proj, string jobID)
        {
            var api = new JobsApi();
            var job = api.GetJob(proj.Owner.Name, proj.Name, jobID.ToString());
            return job;
        }

        private static RecipeInterface GetRecipe(string url)
        {
            Helper.GetRecipeFromRecipeSourceURL(url, out var recipe);
            return recipe;
        }


        public override string ToString()
        {
            return $"CLOUD:{this.Project.Owner.Name}/{this.Project.Name}/{this.JobID}";
        }
        

        public async Task<string> WatchJobStatusAsync(Action<string> progressAction = default, System.Threading.CancellationToken cancelToken = default)
        {
            var api = new JobsApi();
            var proj = this.Project;
            var jobId = this.JobID;

            var cloudJob = api.GetJob(proj.Owner.Name, proj.Name, jobId);
            var status = cloudJob.Status;
            var startTime = status.StartedAt;
            while (status.FinishedAt <= status.StartedAt || status.Status == "Post-Processing")
            {
                var currentSeconds = Math.Round((DateTime.UtcNow - startTime).TotalSeconds);
                // wait 5 seconds before calling api to re-check the status
                var totalDelaySeconds = status.Status == "Scheduled" ? 3 : 5;

                var running = status.RunsPending + status.RunsRunning;
                var done = status.RunsFailed + status.RunsCompleted;
                for (int i = 0; i < totalDelaySeconds; i++)
                {
                    // suspended by user
                    cancelToken.ThrowIfCancellationRequested();

                    var timer = GetUserFriendlyTimeCounter(TimeSpan.FromSeconds(currentSeconds));
                    var message = $"{status.Status}: [{running}/{done}]\n{timer}";
                    progressAction?.Invoke(message);

                    await Task.Delay(1000);
                    currentSeconds++;
                }
                // suspended by user
                cancelToken.ThrowIfCancellationRequested();

                // update status
                await Task.Delay(1000);
                cloudJob = api.GetJob(proj.Owner.Name, proj.Name, jobId);
                status = cloudJob.Status;
                //_simulation = new Simulation(proj, simuId);
            }
            this.CloudJob = cloudJob;
            // suspended by user
            cancelToken.ThrowIfCancellationRequested();

            var totalTime = status.FinishedAt - startTime;
            var finishMessage = status.Status == "Succeeded" ? $"✔ {status.Status}" : $"❌ {status.Status}";
            //progressAction?.Invoke($"Task: {status.Status}");

            finishMessage = $"{finishMessage}: [{GetUserFriendlyTimeCounter(totalTime)}]";
            progressAction?.Invoke(finishMessage);
            return finishMessage;

            string GetUserFriendlyTimeCounter(TimeSpan timeDelta)
            {
                string format = @"hh\:mm\:ss";
                if (timeDelta.Days > 0)
                    format = @"d\ hh\:mm\:ss";
                else if (timeDelta.Hours > 0)
                    format = @"hh\:mm\:ss";
                else if (timeDelta.Minutes > 0)
                    format = @"mm\:ss";
                else
                    format = @"ss";
                return timeDelta.ToString(format);
            }
        }

        public void StopSimulaiton()
        {
            var proj = this.Project;
            var simuId = this.JobID;

            //var api = new JobsApi();
            //api.(proj.Owner.Name, proj.Name, simuId);
        }


    }
}