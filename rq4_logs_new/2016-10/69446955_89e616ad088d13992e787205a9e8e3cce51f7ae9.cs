using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KincoLightManager
{
    public class CommandEventArg : EventArgs
    {
        public object CommandType { get; set; }
        public string Command { get; set; }
    }

    /// <summary>
    /// 步科帶燈貨架控制命令產生中心
    /// Create By  : Enzhi.Zhou
    /// Create Date: 2016/09/27
    /// </summary>
    public class Boss
    {
        public event EventHandler<CommandEventArg> AfterCommandSend;

        private void FireAfterCommandSendEvent(string command,object cmdType)
        {
            if (this.AfterCommandSend != null)
                this.AfterCommandSend(this, new CommandEventArg { Command = command,CommandType=cmdType });
        }

        public event EventHandler<CommandEventArg> BeforeCommandSend;

        private void FireBeforeCommandSendEvent(string command, object cmdType)
        {
            if (this.BeforeCommandSend != null)
                this.BeforeCommandSend(this, new CommandEventArg { Command = command, CommandType = cmdType });
        }

        public static void SendCommand(string cmd)
        {
            CommandCenter.Broadcast(cmd);
        }

        public void SendRackControlCmd(RackTowerLedStatus rackStatus, params UInt16[] racks)
        {
            string cmd = CommandCenter.CreateRackControlCmd(rackStatus, racks);
            FireBeforeCommandSendEvent(cmd,rackStatus);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd,rackStatus);
        }

        public void SendLedOnOffControlCmd(LedOnOffStatus lightStatus, params KeyValuePair<UInt16, UInt16>[] rackNo_lightNo_Pairs)
        {
            string cmd = CommandCenter.CreateLedOnOffControlCmd(lightStatus, rackNo_lightNo_Pairs);
            FireBeforeCommandSendEvent(cmd,lightStatus);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, lightStatus);
        }

        public void SendLedFlashControlCmd(LedFlashStatus flashStatus, UInt16 lightsOnTime, UInt16 lightsOffTime, UInt16 flashCount, params KeyValuePair<UInt16, UInt16>[] rackNo_lightNo_Pairs)
        {
            string cmd = CommandCenter.CreateLedFlashControlCmd(flashStatus, lightsOnTime, lightsOffTime, flashCount, rackNo_lightNo_Pairs);
            FireBeforeCommandSendEvent(cmd, flashStatus);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, flashStatus);
        }

        public void TurnOffRackLedAndTowerWhiteLed(params UInt16[] racks)
        {
            string cmd = CommandCenter.CreateCmd_TurnOffRackLedAndTowerWhiteLed(racks);
            FireBeforeCommandSendEvent(cmd, RackTowerLedStatus.RackLedOff_TowerWhiteOff);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, RackTowerLedStatus.RackLedOff_TowerWhiteOff);
        }

        public void TurnOnRackGreenLedAndTowerWhiteLed(params UInt16[] racks)
        {
            string cmd = CommandCenter.CreateCmd_TurnOnRackGreenLedAndTowerWhiteLed(racks);
            FireBeforeCommandSendEvent(cmd, RackTowerLedStatus.RackGreenOn_TowerWhiteOn);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, RackTowerLedStatus.RackGreenOn_TowerWhiteOn);
        }

        public void TurnOnRackRedLedAndTowerWhiteLed(params UInt16[] racks)
        {
            string cmd = CommandCenter.CreateCmd_TurnOnRackRedLedAndTowerWhiteLed(racks);
            FireBeforeCommandSendEvent(cmd,RackTowerLedStatus.RackRedOn_TowerWhiteOn);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd,RackTowerLedStatus.RackRedOn_TowerWhiteOn);
        }

        public void TurnOffTowerRedAndGreenLed(params UInt16[] racks)
        {
            string cmd = CommandCenter.CreateCmd_TurnOffTowerRedAndGreenLed(racks);
            FireBeforeCommandSendEvent(cmd, RackTowerLedStatus.Tower_RedOff_GreenOff);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, RackTowerLedStatus.Tower_RedOff_GreenOff);
        }

        public void TurnOnTowerRedLed(params UInt16[] racks)
        {
            string cmd = CommandCenter.CreateCmd_TurnOnTowerRedLed(racks);
            FireBeforeCommandSendEvent(cmd, RackTowerLedStatus.Tower_RedOn);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, RackTowerLedStatus.Tower_RedOn);
        }

        public void TurnOnTowerGreeLed(params UInt16[] racks)
        {
            string cmd = CommandCenter.CreateCmd_TurnOnTowerGreeLed(racks);
            FireBeforeCommandSendEvent(cmd, RackTowerLedStatus.Tower_GreenOn);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, RackTowerLedStatus.Tower_GreenOn);
        }

        public void TurnOffTowerRedGreenLed(params UInt16[] racks)
        {
            string cmd = CommandCenter.CreateCmd_TurnOffTowerRedGreenLed(racks);
            FireBeforeCommandSendEvent(cmd, RackTowerLedStatus.Tower_RedOff_GreenOff);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd,RackTowerLedStatus.Tower_RedOff_GreenOff);
        }

        public void TurnOffLed(params KeyValuePair<UInt16, UInt16>[] rackAddress_LEDAddress_Pairs)
        {
            string cmd = CommandCenter.CreateCmd_TurnOffLed(rackAddress_LEDAddress_Pairs);
            FireBeforeCommandSendEvent(cmd, LedOnOffStatus.Off);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, LedOnOffStatus.Off);
        }

        public void TurnOnGreenLed(params KeyValuePair<UInt16, UInt16>[] rackAddress_LEDAddress_Pairs)
        {
            string cmd = CommandCenter.CreateCmd_TurnOnGreenLed(rackAddress_LEDAddress_Pairs);
            FireBeforeCommandSendEvent(cmd, LedOnOffStatus.GreenOn);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, LedOnOffStatus.GreenOn);
        }

        public void TurnOnRedLed(params KeyValuePair<UInt16, UInt16>[] rackAddress_LEDAddress_Pairs)
        {
            string cmd = CommandCenter.CreateCmd_TurnOnRedLed(rackAddress_LEDAddress_Pairs);
            FireBeforeCommandSendEvent(cmd, LedOnOffStatus.RedOn);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, LedOnOffStatus.RedOn);
        }

        public void SendLightsFlashControlCmd(LedFlashStatus flashStatus, UInt16 lightsOnTime, UInt16 lightsOffTime, UInt16 flashCount, params KeyValuePair<UInt16, UInt16>[] rackNo_lightNo_Pairs)
        {
            string cmd = CommandCenter.CreateLightsFlashControlCmd(flashStatus, lightsOnTime, lightsOffTime, flashCount, rackNo_lightNo_Pairs);
            FireBeforeCommandSendEvent(cmd, flashStatus);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, flashStatus);
        }

        public void FlashGreenLed(UInt16 lightsOnTime, UInt16 lightsOffTime, UInt16 flashCount, params KeyValuePair<UInt16, UInt16>[] rackNo_lightNo_Pairs)
        {
            string cmd = CommandCenter.CreateCmd_FlashGreenLed(lightsOnTime, lightsOffTime, flashCount, rackNo_lightNo_Pairs);
            FireBeforeCommandSendEvent(cmd, LedFlashStatus.Green_Flash);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, LedFlashStatus.Green_Flash);
        }

        public void FlashRedLed(UInt16 lightsOnTime, UInt16 lightsOffTime, UInt16 flashCount, params KeyValuePair<UInt16, UInt16>[] rackNo_lightNo_Pairs)
        {
            string cmd = CommandCenter.CreateCmd_FlashRedLed(lightsOnTime, lightsOffTime, flashCount, rackNo_lightNo_Pairs);
            FireBeforeCommandSendEvent(cmd, LedFlashStatus.Red_Flash);
            Boss.SendCommand(cmd);
            FireAfterCommandSendEvent(cmd, LedFlashStatus.Red_Flash);
        }

        public static string GetLogMessage(object cmdType, string cmd)
        {
            string template = "{0}  發送【{1}】指令:{2}";
            string cmdTpStr = "";
            if (cmdType.GetType() == typeof(LedFlashStatus))
            {
                LedFlashStatus flashType = (LedFlashStatus)cmdType;
                switch (flashType)
                {
                    case LedFlashStatus.Green_Flash:
                        cmdTpStr = "綠燈閃爍";
                        break;
                    case LedFlashStatus.Red_Flash:
                        cmdTpStr = "紅燈閃爍";
                        break;
                    default:
                        break;
                }
            }
            else if (cmdType.GetType() == typeof(LedOnOffStatus))
            {
                LedOnOffStatus onOffType = (LedOnOffStatus)cmdType;
                switch (onOffType)
                {
                    case LedOnOffStatus.Off:
                        cmdTpStr = "指示燈滅";
                        break;
                    case LedOnOffStatus.GreenOn:
                        cmdTpStr = "綠燈亮";
                        break;
                    case LedOnOffStatus.RedOn:
                        cmdTpStr = "紅燈亮";
                        break;
                    default:
                        break;
                }
            }
            else {
                RackTowerLedStatus status = (RackTowerLedStatus)cmdType;
                switch (status)
                {
                    case RackTowerLedStatus.RackLedOff_TowerWhiteOff:
                        cmdTpStr = "貨位燈全滅";
                        break;
                    case RackTowerLedStatus.RackGreenOn_TowerWhiteOn:
                        cmdTpStr = "貨位綠燈全亮";
                        break;
                    case RackTowerLedStatus.RackRedOn_TowerWhiteOn:
                        cmdTpStr = "貨位紅燈全亮";
                        break;
                    case RackTowerLedStatus.Tower_RedOff_GreenOff:
                        cmdTpStr = "貨架紅綠燈塔滅";
                        break;
                    case RackTowerLedStatus.Tower_RedOn:
                        cmdTpStr = "貨架紅燈塔亮";
                        break;
                    case RackTowerLedStatus.Tower_GreenOn:
                        cmdTpStr = "貨架綠燈塔亮";
                        break;
                    case RackTowerLedStatus.Tower_RedOn_GreenOn:
                        cmdTpStr = "貨架紅綠燈塔亮";
                        break;
                    default:
                        break;
                }
            }

            return template.FormatWith(DateTime.Now.ToLongTimeString(), cmdTpStr, cmd);
        }

    }
}