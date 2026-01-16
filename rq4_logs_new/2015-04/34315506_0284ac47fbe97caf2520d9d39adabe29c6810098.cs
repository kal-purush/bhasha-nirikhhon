using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MouseThingy
{
    public class MouseThingy
    {
        private static frmMouseThingy mainForm = null;

        public static frmMouseThingy MainForm
        {
            get { return mainForm; }
            set { mainForm = value; }
        }

        private static float rummageProgress = 0;

        public static float RummageProgress
        {
            get { return MouseThingy.rummageProgress; }
            set {
                MouseThingy.rummageProgress = value;
                MainForm.barRummageProgress.Value = (int)(value*100);
            }
        }

        public const uint FOV_MULTIPLIER_OFFSET = 4315524;
        public const uint CURRENT_FOV_OFFSET = 4883752;

        private static float sensitivityDivisor = 4000;

        public static float SensitivityDivisor
        {
            get { return MouseThingy.sensitivityDivisor; }
        }

        private static uint mpViewOffsetAddress = 0;

        public static uint ViewOffsetAddress
        {
            get { return MouseThingy.mpViewOffsetAddress; }
            set { MouseThingy.mpViewOffsetAddress = value; }
        }
    }
}