using Production.MediaProcess;
using System;
using System.Collections.Generic;
using System.Text;

namespace Production.Model
{
    public class CutEffect : IEffect
    {
        private TimeSpan Begin { get; set; }
        private TimeSpan End { get; set; }
        private VideoHandlerObject videoHandler { get; set; }

        /// <summary>
        /// 影片剪裁
        /// </summary>
        /// <param name="begin">開始時間</param>
        /// <param name="end">結束時間</param>
        public CutEffect(TimeSpan begin, TimeSpan end)
        {
            this.Begin = begin;
            this.End = end;
        }

        public string OutputPath { get; private set; }

        public void SetDataSource(Guid guid, string path)
        {
            videoHandler = VideoHandler.SetSource(guid, path);
            this.OutputPath = path;
        }

        public void Excute()
        {
            this.videoHandler.CutVideo(this.Begin, this.End);
            this.OutputPath = videoHandler.OutputPath;
        }
    }
}