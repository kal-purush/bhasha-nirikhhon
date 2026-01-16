using IHGVM_VideoManeuver;
using System;
using System.Collections;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;

namespace IHGVM_VideoSplitter
{    
    public class FrameWriter
    {
        private ProcessFrame processFrame;
        private string videoPath = "";

        public VideoStrech VideoStrech
        {
            get
            {
                if (processFrame.FrameReader != null)
                    return processFrame.FrameReader.VideoStrech;
                return null;
            }
        }

        public FrameWriter(ProcessFrame processFrame) 
        {
            this.processFrame = processFrame;
            Initialize();
        }

        public void Initialize()
        {
            string basePath = @"..\Activities\", finalPath = basePath + processFrame.FileName + @"\";
            if (!Directory.Exists(finalPath))
                Directory.CreateDirectory(finalPath);

            this.videoPath = finalPath;
        }

        public void SaveActivity()
        {
            for (int i = 0; i < processFrame.clearedList.Count; i++)
            {
                ExportActivity(processFrame.clearedList[i].FrameNumber, this.GetEndFrame(i), i);
            }
        }

        public void SaveActivity(int index)
        {
            if (processFrame.clearedList.Count > index)
            {
                if (VideoStrech != null)
                    ExportActivity(processFrame.clearedList[index].FrameNumber, this.GetEndFrame(index), index);
            }
        }

        public void ExportActivity(int startFrame, int endFrame, int videoIndex)
        {
            Bitmap bitFrame = VideoStrech.GetSubsequentPosition(startFrame);
            VideoRepel repel = new VideoRepel(videoPath + videoIndex + ".avi", VideoStrech.FrameRate, bitFrame);

            for (int i = startFrame + 1; i <= endFrame; i++)
            {
                bitFrame = VideoStrech.GetSubsequentPosition(i);
                repel.AddFrame(bitFrame);
            }

            repel.Close();
        }

        public int GetEndFrame(int index)
        {
            return (index < (processFrame.clearedList.Count - 1)) ? processFrame.clearedList[index + 1].FrameNumber : (processFrame.FrameLength - 1);
        }

    }

}