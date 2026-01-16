using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

namespace spnavwrapper
{
    public sealed class SpaceNav
    {
        double _sensitivity = 1.0;
        int _threshold = 5;

        static SpaceNav instance = null;
        static readonly object padlock = new object();

        const int SPNAV_EVENT_MOTION = 1;
        const int SPNAV_EVENT_BUTTON = 2;
        const string DLL_NAME = "spnavhdi";

        struct SpNavEventMotion
        {
            public int type;
            public int x, y, z;
            public int rx, ry, rz;
            public uint period;
            public IntPtr data;
        }

        struct SpNavEventButton
        {
            public int type;
            public int press;
            public int bnum;
        }

        [StructLayout(LayoutKind.Explicit)]
        struct SpNavEvent
        {
            [FieldOffset(0)] public int type;
            [FieldOffset(0)] public SpNavEventMotion motion;
            [FieldOffset(0)] public SpNavEventButton button;
        }

        [DllImport(DLL_NAME, CallingConvention = CallingConvention.Cdecl)]
        static extern int spnav_open();
        [DllImport(DLL_NAME, CallingConvention = CallingConvention.Cdecl)]
        static extern int spnav_close();
        [DllImport(DLL_NAME, CallingConvention = CallingConvention.Cdecl)]
        static extern int spnav_wait_event(ref SpNavEvent ev);
        [DllImport(DLL_NAME, CallingConvention = CallingConvention.Cdecl)]
        static extern int spnav_wait_event_timeout(ref SpNavEvent ev, int timeout);
        [DllImport(DLL_NAME, CallingConvention = CallingConvention.Cdecl)]
        static extern int spnav_sensitivity(double sens);
        [DllImport(DLL_NAME, CallingConvention = CallingConvention.Cdecl)]
        static extern int spnav_deadzone(int threshold);

        SpaceNav()
		{
			// TODO : handle retcode
			spnav_open();
		}

		~SpaceNav()
		{
			// TODO : handle retcode
			spnav_close();
		}

		public static SpaceNav Instance
		{
			get
			{
				lock (padlock)
				{
					if (instance == null)
					{
                        instance = new SpaceNav();
					}
					return instance;
				}
			}
		}

		public void CloseDevice()
		{
			lock (padlock)
			{
				spnav_close();
				instance = null;
			}
		}

		public SpaceNavEvent WaitEvent()
		{
			lock (padlock)
			{
				SpNavEvent sev = new SpNavEvent();
				spnav_wait_event(ref sev);
				switch (sev.type)
				{
					case SPNAV_EVENT_BUTTON:
						return new SpaceNavButtonEvent(Convert.ToBoolean(sev.button.press), sev.button.bnum);
					case SPNAV_EVENT_MOTION:
						var ev = sev.motion;
						return new SpaceNavMotionEvent(ev.x, ev.y, ev.z, ev.rx, ev.ry, ev.rz);
					default:
						return null;
				}
			}
		}

		public SpaceNavEvent WaitEvent(int milliseconds)
		{
			lock (padlock)
			{
				SpNavEvent sev = new SpNavEvent();
				int ev_type = spnav_wait_event_timeout(ref sev, milliseconds);
				switch (ev_type)
				{
					case SPNAV_EVENT_BUTTON:
						return new SpaceNavButtonEvent(Convert.ToBoolean(sev.button.press), sev.button.bnum);
					case SPNAV_EVENT_MOTION:
						var ev = sev.motion;
						return new SpaceNavMotionEvent(ev.x, ev.y, ev.z, ev.rx, ev.ry, ev.rz);
					default:
						return null;
				}
			}
		}

		public double Sensitivity
        {
            get
			{
				return _sensitivity;
			}

			set
			{
				lock (padlock)
				{
					// TODO : handle retcode
					spnav_sensitivity(value);
					_sensitivity = value;
				}
			}
		}

		public int Threshold
        {
			get
			{
				return _threshold;
			}
            set
			{
				lock (padlock)
				{
					// TODO : handle retcode
					spnav_deadzone(value);
					_threshold = value;
				}
			}
        }
	}
}