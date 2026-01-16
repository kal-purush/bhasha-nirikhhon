namespace Giverspace {
    using UnityEngine;
    using System;
    using System.IO;
    using System.Threading;

    public enum Severity : byte {
        Info = 0,
        Error = 1,
        Debug = 2
    }

    public enum ButtonPressed : byte {
        X,
        Y,
        A,
        B,
        RTrigger,
        LTrigger
    }

    public enum PlayerNumber : byte
    {
        Brawler,
        Shooter,
        Mage
    }

    public class Log {
        // Note: You may need to change this number to suit your needs:
        const int BUFFER_SIZE = 100;
        const int FLUSH_SIZE = BUFFER_SIZE/2;

        enum LogType {
            String,
            PlayerPosRot,
            InputPressed,
            DamageDealt,
            DamageTaken
            // TODO: add more entries if you need more message types
        }

        private struct LogMsg {
            public LogType LogType;
            public DateTime ReportedAt;
            public Severity Seriousness;

            // For String
            public string Message;

            // For PlayerPosRot
            public Vector3 Position;
            public Vector3 Orientation;

            // For InputPressed
            public ButtonPressed ButtonPressed;
            public PlayerNumber Player;

            // For total Damage Dealt
            public int DamageDealt;

            // For total Damage Taken
            public int DamageTaken;

            // TODO: add more fields if you need to track more types of data

            void WriteFieldWith (string fieldname, object fieldvalue, StreamWriter w) {
                w.Write('"');
                w.Write(fieldname);
                w.Write('"');
                w.Write(':');
                w.Write('"');
                w.Write(fieldvalue);
                w.Write('"');
            }

            void WriteFieldWith (string fieldname, int fieldvalue, StreamWriter w) {
                w.Write('"');
                w.Write(fieldname);
                w.Write('"');
                w.Write(':');
                w.Write('"');
                w.Write(fieldvalue);
                w.Write('"');
            }

            void WriteTypeWith (string typename, StreamWriter w) {
                w.Write("\"t\":");
                w.Write('"');
                w.Write(typename);
                w.Write('"');
            }

            void WriteTimeStampWith (StreamWriter w) {
                w.Write("\"ts\":");
                w.Write(ReportedAt.Ticks);
            }

            void WriteVectorWith (ref Vector3 v, string name, StreamWriter w) {
                w.Write('"');
                w.Write(name);
                w.Write('"');
                w.Write(':');
                w.Write('[');
                w.Write(v.x);
                w.Write(',');
                w.Write(v.y);
                w.Write(',');
                w.Write(v.z);
                w.Write(']');
            }

            public void WriteWith (StreamWriter w) {
                switch (LogType) {
                    case LogType.String:
                        w.Write('{');
                        WriteTimeStampWith(w);
                        w.Write(',');
                        WriteTypeWith("info",w);
                        w.Write(',');
                        WriteFieldWith("m",Message,w);
                        w.WriteLine('}');
                        break;
                    case LogType.PlayerPosRot:
                        w.Write('{');
                        WriteTimeStampWith(w);
                        w.Write(',');
                        WriteTypeWith("posrot",w);
                        w.Write(',');
                        WriteVectorWith(ref Position, "pos", w);
                        w.Write(',');
                        WriteVectorWith(ref Orientation, "rot", w);
                        w.WriteLine('}');
                        break;
                    case LogType.InputPressed:
                        w.Write('{');
                        WriteTimeStampWith(w);
                        w.Write(',');
                        WriteTypeWith("playernum", w);
                        w.Write(',');
                        WriteFieldWith("p", Player, w);
                        w.Write(',');
                        WriteTypeWith("inputpressed", w);
                        w.Write(',');
                        WriteFieldWith("b", ButtonPressed, w);
                        w.WriteLine('}');
                        break;
                    case LogType.DamageDealt:
                        w.Write('{');
                        WriteTimeStampWith(w);
                        w.Write(',');
                        WriteTypeWith("playernum", w);
                        w.Write(',');
                        WriteFieldWith("p", Player, w);
                        w.Write(',');
                        WriteTypeWith("damagedealt", w);
                        w.Write(',');
                        WriteFieldWith("d", DamageDealt, w);
                        w.WriteLine('}');
                        break;
                    case LogType.DamageTaken:
                        w.Write('{');
                        WriteTimeStampWith(w);
                        w.Write(',');
                        WriteTypeWith("playernum", w);
                        w.Write(',');
                        WriteFieldWith("p", Player, w);
                        w.Write(',');
                        WriteTypeWith("damagetaken", w);
                        w.Write(',');
                        WriteFieldWith("d", DamageTaken, w);
                        w.WriteLine('}');
                        break;
                        // TODO: Handle more message types here by adding more cases
                }
            }
        }

        // Singleton Instance
        private static Log _instance;

        public static Log Metrics {
            get { return _instance; }
        }

        static Log () {
            var lifetimeHelper = new GameObject("+LoggerLifetime");
            lifetimeHelper.AddComponent<LoggerLifetime>();
            _instance = new Log();
            _instance.Message("Started");
            _instance.Start();
        }

        ~Log () {
            Exit();
        }

        public static void Exit () {
            if (_instance != null) {
                _instance.Message("Stopped");
                _instance.Flush();
                _instance = null;
            }
        }

        int _messageBufferPointer = 0;
        static LogMsg[] _messageBuffer = new LogMsg[BUFFER_SIZE];
        static LogMsg[] _messageWriteBuffer = new LogMsg[BUFFER_SIZE];
        static string _logFileName;

        void Enqueue (LogMsg message) {
            _messageBuffer[_messageBufferPointer] = message;
            _messageBufferPointer++;

            if (_messageBufferPointer > FLUSH_SIZE) {
                Flush();
            }
        }


        private static AutoResetEvent tellConsumerToConsume = new AutoResetEvent(false);
        static int _newMessageCount = 0;
        private static void ConsumeItem() {
            using (TextWriter w = new StreamWriter (_logFileName, true)) {
                while(tellConsumerToConsume.WaitOne())
                {
                    for (int i = 0; i < _newMessageCount; i++) {
                        LogMsg m = _messageWriteBuffer[i];
                        m.WriteWith((StreamWriter)w);
                    }
                    w.Flush();
                }
            }
        }

        public void Flush () {
            _messageWriteBuffer = Interlocked.Exchange(ref _messageBuffer, _messageWriteBuffer);
            _newMessageCount = Interlocked.Exchange(ref _messageBufferPointer, 0);
            tellConsumerToConsume.Set();
        }


        // TODO: add support for logging additional message types here:
        // you can then access them via Log.Metrics.FunctionName
        public void PlayerPosRotMessage (Vector3 position, Vector3 orientation) {
            Enqueue(new LogMsg { ReportedAt = DateTime.UtcNow,
                                 Position = position,
                                 Orientation = orientation,
                                 Seriousness = Severity.Info,
                                 LogType = LogType.PlayerPosRot });
        }

        public void Message (string msg) {
            Enqueue(new LogMsg { ReportedAt = DateTime.UtcNow,
                                 Message = msg,
                                 Seriousness = Severity.Info,
                                 LogType = LogType.String });
        }

        public void ButtonPressedMessage(ButtonPressed btnPrs, PlayerNumber playnum)
        {
            Enqueue(new LogMsg
            {
                ReportedAt = DateTime.UtcNow,
                ButtonPressed = btnPrs,
                Player = playnum,
                Seriousness = Severity.Info,
                LogType = LogType.InputPressed
            });
        }

        public void TotalDamageDealt(int dmgdlt, PlayerNumber playnum)
        {
            Enqueue(new LogMsg
            {
                ReportedAt = DateTime.UtcNow,
                Player = playnum,
                DamageDealt = dmgdlt,
                Seriousness = Severity.Info,
                LogType = LogType.DamageDealt
            });
        }

        public void TotalDamageTaken(int dmgtk, PlayerNumber playnum)
        {
            Enqueue(new LogMsg
            {
                ReportedAt = DateTime.UtcNow,
                Player = playnum,
                DamageTaken = dmgtk,
                Seriousness = Severity.Info,
                LogType = LogType.DamageTaken
            });
        }

        private void Start (string fileName = "", bool oneLogPerProcess = false) {
            // Unique FileName with date in it. And ProcessId so the same process running twice will log to different files
            string lp = oneLogPerProcess ? "_" + System.Diagnostics.Process.GetCurrentProcess().Id : "";
            _logFileName = (fileName == "") ? string.Format("{0}{1}{2}-{3}-{4}_{5}",
                                                            Application.persistentDataPath,
                                                            Path.DirectorySeparatorChar,
                                                            DateTime.Now.Year.ToString("0000"),
                                                            DateTime.Now.Month.ToString("00"),
                                                            DateTime.Now.Day.ToString("00"),
                                                            lp) : fileName;
            int i = 0;
            while (File.Exists(string.Format("{0}{1}.log",_logFileName,i))) {
                i++;
            }
            _logFileName = string.Format("{0}{1}.log",_logFileName,i);
            Debug.Log(string.Format("Logging Metrics to {0}",_logFileName));

            var t_Consumer = new Thread(new ThreadStart(ConsumeItem));
            t_Consumer.IsBackground = true;
            t_Consumer.Start();
        }
    }
}