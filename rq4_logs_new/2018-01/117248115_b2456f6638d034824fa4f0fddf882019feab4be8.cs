using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TBGResearch.Classes;

namespace TBGResearch.Logic
{
    /// <summary>
    /// A class containing the logic for assigning teasm
    /// </summary>
    public static class Allocator
    {
        /// <summary>
        /// 
        /// </summary>
        public static void AllocationMaster()
        {
            foreach (Entity nation in Master.MasterEntityList)
            {
                
            }
        }

        public static void Cleanup(ProgressFrame frame, TechTree researcher)
        {
            // If all Lines are complete, we can mark the frame itself as complete.
            if (frame.Lines.All(x => x.Progress >= x.Line.RequiredResearch))
            {
                frame.IsComplete = true;
                frame.IsActive = false;
                frame.IsAccessible = false;
            }
        }

        public static void CheckAccessible(ResearchResult status)
        {
            Dictionary<ResearchFrame, ProgressFrame> matchup = new Dictionary<ResearchFrame, ProgressFrame>();
            status.UpdateFrames.Values.ToList().ForEach(x => matchup.Add(x.ParentFrame, x));
            EntityStateOfArt currentProgress = status.Parent.Status;

            foreach (KeyValuePair<ResearchFrame, ProgressFrame> pair in matchup)
            {
                ResearchFrame core = pair.Key;
                ProgressFrame active = pair.Value;

                if (active.IsComplete) // This will only be present if it was actively worked on this turn, so any IsComplete means it completed this turn
                {
                    foreach (ResearchFrame target in core.LeadsToFrames)
                    {
                        ProgressFrame targetMatch = currentProgress.Frames.First(x => x.IdTag == target.IdTag);
                        if (!targetMatch.IsComplete && !targetMatch.IsAccessible)
                        {
                            List<ProgressFrame> prereqs = currentProgress.Frames.Where(r => r.ParentFrame.PrereqFrames.Contains(target)).ToList();

                            if (prereqs.All(x => x.IsComplete))
                                targetMatch.IsAccessible = true;
                        }
                    }
                }
            }
        }
    }
}