/*
Copyright 2012 HighVoltz

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Windows.Media;
using HighVoltz.HBRelog.Tasks;
using HighVoltz.HBRelog.Settings;
using System.Threading.Tasks;
using System;

namespace HighVoltz.HBRelog
{
    sealed public class CharacterProfile : INotifyPropertyChanged
    {
        public ProfileSettings Settings { get; set; }
        public bool IsRunning { get; private set; }
        public bool IsPaused { get; private set; }
        public readonly TaskManager TaskManager;
        public ObservableCollection<BMTask> Tasks { get; private set; }

        public CharacterProfile()
        {
            Settings = new ProfileSettings();
            Tasks = new ObservableCollection<BMTask>();
            TaskManager = new TaskManager(this);
            ShoudPause = false;
        }
        private string _status;
        /// <summary>
        /// Status message
        /// </summary>
        public string Status
        {
            get { return _status; }
            set { _status = value; NotifyPropertyChanged("Status"); }
        }
        private string _tooltip;
        /// <summary>
        /// Tooltip message
        /// </summary>
        public string Tooltip
        {
            get { return _tooltip; }
            set
            {
                if (value != _tooltip)
                {
                    _tooltip = value;
                    NotifyPropertyChanged("Tooltip");
                }
            }
        }

        private int _pauseCount;
        /// <summary>
        ///     WoW is disconnected/crashed several times in succession
        /// </summary>
        public bool ShoudPause
        {
            get
            {
                //check for 3-5 pause request
                //Random rnd = new Random();
                return _pauseCount > 5; //3 + rnd.Next(3);
            }
            internal set
            {
                if (value)
                {
                    //increase pause counter
                    _pauseCount++;
                }
                else
                    //if at least one false request required
                    _pauseCount = 0;
            }
        }

        private string _taskTooltip;
        /// <summary>
        /// Current Task Tooltip message. Displayed in main ToolTip
        /// </summary>
        public string TaskTooltip
        {
            get { return _taskTooltip; }
            set
            {
                if (value != _taskTooltip)
                {
                    _taskTooltip = value;
                    UpdateTooltip();
                }
            }
        }

        private string _botInfoTooltip;
        /// <summary>
        /// Bot info Tooltip message. Displayed in main ToolTip
        /// </summary>
        public string BotInfoTooltip
        {
            get { return _botInfoTooltip; }
            set
            {
                if (value != _botInfoTooltip)
                {
                    _botInfoTooltip = value;
                    UpdateTooltip();
                }
            }
        }

        void UpdateTooltip()
        {
            Tooltip = string.Format("{0}{1}",
                        !string.IsNullOrEmpty(TaskTooltip) ? TaskTooltip + "\n" : null,
                        BotInfoTooltip);
        }

        public void Pulse()
        {
            if (ShoudPause)
            {
                ShoudPause = false;
                Random rnd = new Random();
                Pause(20 + rnd.Next(20));
            }
                
            if (IsRunning && !IsPaused)
            {
                TaskManager.Pulse();
            }
        }

        public void Pause()
        {
            Status = "Paused";
            if (TaskManager.WowManager.LockToken != null && TaskManager.WowManager.LockToken.IsValid)
            {
                TaskManager.WowManager.LockToken.ReleaseLock();
            }

            IsPaused = true;
        }

        public void Pause(int minutes)
        {
            Status = "Paused for " + minutes + " minutes";
            if (TaskManager.WowManager.LockToken != null && TaskManager.WowManager.LockToken.IsValid)
            {
                TaskManager.WowManager.LockToken.ReleaseLock();
            }

            IsPaused = true;

            // wait ms to resume bot
            Task.Run(async delegate
            {
                await Task.Delay(minutes * 1000 * 60);
                if (!IsPaused)
                    return;
                Status = "Running";
                IsRunning = true;
                IsPaused = false;
            });
        }
        public void Start()
        {
            Status = "Running";
            if (!IsPaused)
                TaskManager.Start();
            IsRunning = true;
            IsPaused = false;
        }

        public void Stop()
        {
            Status = "Stopped";
            TaskManager.Stop();
            IsRunning = false;
            IsPaused = false;
        }

        private string _lastLog;
        public void Log(string format, params object[] args)
        {
            var msg = string.Format(format, args);
            if (msg == _lastLog)
                return;
            _lastLog = msg;

            if (HbRelogManager.Settings.UseDarkStyle)
                HBRelog.Log.Write(Colors.LightBlue, Settings.ProfileName + ": ", Colors.LightGreen, "{0}", msg);
            else
                HBRelog.Log.Write(Colors.DarkSlateBlue, Settings.ProfileName + ": ", Colors.DarkGreen, "{0}", msg);
        }

        public void Err(string format, params object[] args)
        {
            HBRelog.Log.Write(HbRelogManager.Settings.UseDarkStyle ? Colors.LightBlue : Colors.DarkSlateBlue,
                                        Settings.ProfileName + ": ", Colors.Red, format, args);
        }

        public CharacterProfile ShadowCopy()
        {
            var cp = (CharacterProfile)MemberwiseClone();
            cp.Tasks = new ObservableCollection<BMTask>();
            foreach (var bmTask in Tasks)
            {
                cp.Tasks.Add(bmTask.ShadowCopy());
            }
            cp.Settings = Settings.ShadowCopy();
            return cp;
        }

        public event PropertyChangedEventHandler PropertyChanged;
        private void NotifyPropertyChanged(string name)
        {
            if (PropertyChanged != null)
            {
                PropertyChanged(this, new PropertyChangedEventArgs(name));
            }
        }
    }
}