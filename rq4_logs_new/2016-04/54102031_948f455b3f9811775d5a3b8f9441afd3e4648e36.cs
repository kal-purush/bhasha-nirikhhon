using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Phidgets.Events;
using Phidgets;

namespace ICT4Rails.Logic
{
    public class RFID
    {

       



        #region Methods - RFID
        /// <summary>
        /// Recieve RFID messages
        /// </summary>
        private void openCmdLine(Phidget p)
        {
            openCmdLine(p, null);
        }

        /// <summary>
        /// Recieve RFID messages part 2
        /// </summary>
        private void openCmdLine(Phidget p, String pass)
        {
            int serial = -1;
            String logFile = null;
            int port = 5001;
            String host = null;
            bool remote = false, remoteIP = false;
            string[] args = Environment.GetCommandLineArgs();
            String appName = args[0];

            try
            { //Parse the flags
                for (int i = 1; i < args.Length; i++)
                {
                    if (args[i].StartsWith("-"))
                        switch (args[i].Remove(0, 1).ToLower())
                        {
                            case "l":
                                logFile = (args[++i]);
                                break;
                            case "n":
                                serial = int.Parse(args[++i]);
                                break;
                            case "r":
                                remote = true;
                                break;
                            case "s":
                                remote = true;
                                host = args[++i];
                                break;
                            case "p":
                                pass = args[++i];
                                break;
                            case "i":
                                remoteIP = true;
                                host = args[++i];
                                if (host.Contains(":"))
                                {
                                    port = int.Parse(host.Split(':')[1]);
                                    host = host.Split(':')[0];
                                }
                                break;
                            default:
                                goto usage;
                        }
                    else
                        goto usage;
                }
                if (logFile != null)
                    Phidget.enableLogging(Phidget.LogLevel.PHIDGET_LOG_INFO, logFile);
                if (remoteIP)
                    p.open(serial, host, port, pass);
                else if (remote)
                    p.open(serial, host, pass);
                else
                    p.open(serial);
                return; //success
            }
            catch { }
        usage:
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("Invalid Command line arguments." + Environment.NewLine);
            sb.AppendLine("Usage: " + appName + " [Flags...]");
            sb.AppendLine("Flags:\t-n   serialNumber\tSerial Number, omit for any serial");
            sb.AppendLine("\t-l   logFile\tEnable phidget21 logging to logFile.");
            sb.AppendLine("\t-r\t\tOpen remotely");
            sb.AppendLine("\t-s   serverID\tServer ID, omit for any server");
            sb.AppendLine("\t-i   ipAddress:port\tIp Address and Port. Port is optional, defaults to 5001");
            sb.AppendLine("\t-p   password\tPassword, omit for no password" + Environment.NewLine);
            sb.AppendLine("Examples: ");
            sb.AppendLine(appName + " -n 50098");
            sb.AppendLine(appName + " -r");
            sb.AppendLine(appName + " -s myphidgetserver");
            sb.AppendLine(appName + " -n 45670 -i 127.0.0.1:5001 -p paswrd");
            
        }

        /// <summary>
        /// Tag lost event handler...here we simply want to clear our tag field in the GUI
        /// </summary>
      

        /// <summary>
        /// Tag setter
        /// </summary>
        public void rfid_Tag(object sender, TagEventArgs e)
        {
            string tag = e.Tag;
            

            if (tag == "2800a7cd1a")
            {
                tb_Username.Text = "Jim";
                tb_Password.Text = "Jim123";
                btn_Connect.PerformClick();
            }
            else if (tag == "1c00fd178a")
            {
                tb_Username.Text = "Sven";
                tb_Password.Text = "Sven123";
                btn_Connect.PerformClick();
            }
            else if (tag == "28005e9a58")
            {
                tb_Username.Text = "Lars";
                tb_Password.Text = "Lars123";
                btn_Connect.PerformClick();
            }
          
        }
        #endregion
    }
}