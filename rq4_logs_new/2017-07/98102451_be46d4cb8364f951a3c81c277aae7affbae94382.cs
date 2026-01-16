using System;
using System.Diagnostics.CodeAnalysis;
using System.Drawing;
using System.Windows.Forms;

namespace KeyExamine
{
    public class KeyExamine:Form
    {
        public static void Main()
        {
            Application.Run(new KeyExamine());
        }

        enum EventType
        {
            KeyDown,
            KeyUp,
            KeyPress
        }

        struct KeyEvent
        {
            public EventType EvtType;
            public EventArgs EvtArgs;
        }

        private const int NumLines = 25;
        private int iNumValid;
        private int iInsertIndex;
        private KeyEvent[] aKeyevt = new KeyEvent[NumLines];

        private int xEvent, xChar, xCode, xMods, xData, xShift, xCtrl, xAlt;

        [SuppressMessage("ReSharper", "VirtualMemberCallInConstructor")]
        public KeyExamine()
        {
            Text = "KeyExamine";
            BackColor = SystemColors.Window;
            ForeColor = SystemColors.WindowText;
            xEvent = 0;
            xChar = xEvent + 5 * Font.Height;
            xCode = xChar + Font.Height * 5;
            xMods = xCode + Font.Height * 8;
            xData = xMods + Font.Height * 8;
            xShift = xData + Font.Height * 8;
            xCtrl = xShift + Font.Height * 5;
            xAlt = xCtrl + Font.Height * 5;
            var xRight = xAlt + Font.Height * 5;

            ClientSize = new Size(xRight, Font.Height * (NumLines + 1));
            FormBorderStyle = FormBorderStyle.Fixed3D;
            MaximizeBox = false;
        }

        protected override void OnKeyDown(KeyEventArgs e)
        {
            aKeyevt[iInsertIndex].EvtType=EventType.KeyDown;
            aKeyevt[iInsertIndex].EvtArgs = e;
            OnKey();
        }

        protected override void OnKeyUp(KeyEventArgs e)
        {
            aKeyevt[iInsertIndex].EvtType = EventType.KeyUp;
            aKeyevt[iInsertIndex].EvtArgs = e;
            OnKey();
        }

        protected override void OnKeyPress(KeyPressEventArgs e)
        {
            aKeyevt[iInsertIndex].EvtType = EventType.KeyPress;
            aKeyevt[iInsertIndex].EvtArgs = e;
            OnKey();
        }

        void OnKey()
        {
            if (iNumValid < NumLines)
            {
                Graphics grfx = CreateGraphics();
                DisplayKeyInfo(grfx, iInsertIndex, iInsertIndex);
                grfx.Dispose();
            }
            else
            {
                ScrollLines();
            }
            iInsertIndex = (iInsertIndex + 1) % NumLines;
            iNumValid = Math.Min(iNumValid + 1, NumLines);
        }

        protected virtual void ScrollLines()
        {
            Rectangle rect=new Rectangle(0,Font.Height,ClientSize.Width,ClientSize.Height-Font.Height);
            Invalidate(rect);
        }

        protected override void OnPaint(PaintEventArgs e)
        {
            Graphics grfx = e.Graphics;
            BoldUnderline(grfx, "Event", xEvent, 0);
            BoldUnderline(grfx, "KeyChar", xChar, 0);
            BoldUnderline(grfx, "KeyCode", xCode, 0);
            BoldUnderline(grfx, "Modifiers", xMods, 0);
            BoldUnderline(grfx, "KeyData", xData, 0);
            BoldUnderline(grfx, "Shift", xShift, 0);
            BoldUnderline(grfx, "Control", xCtrl, 0);
            BoldUnderline(grfx, "Alt", xAlt, 0);
            if (iNumValid < NumLines)
            {
                for (int i = 0; i < iNumValid; i++)
                {
                    DisplayKeyInfo(grfx, i, i);
                }
            }
            else
            {
                for (int i = 0; i < NumLines; i++)
                {
                    DisplayKeyInfo(grfx, i, (iInsertIndex + i) % NumLines);
                }
            }
        }

        void BoldUnderline(Graphics grfx, string str, int x, int y)
        {
            Brush brush = new SolidBrush(ForeColor);
            grfx.DrawString(str, Font, brush, x, y);
            grfx.DrawString(str, Font, brush, x + 1, y);
            SizeF sizef = grfx.MeasureString(str, Font);
            grfx.DrawLine(new Pen(ForeColor),x,y+sizef.Height,x+sizef.Width,y+sizef.Height );
        }

        private void DisplayKeyInfo(Graphics grfx, int y, int i)
        {
            Brush br = new SolidBrush(ForeColor);
            y = (1 + y) * Font.Height;
            grfx.DrawString(aKeyevt[i].EvtType.ToString(), Font, br, xEvent, y);
            if (aKeyevt[i].EvtType == EventType.KeyPress)
            {
                KeyPressEventArgs kpea = (KeyPressEventArgs) aKeyevt[i].EvtArgs;
                string str = String.Format("\x202D{0} (0x{1:X4})", kpea.KeyChar, (int) kpea.KeyChar);
                grfx.DrawString(str, Font, br, xChar, y);
            }
            else
            {
                KeyEventArgs kea = (KeyEventArgs)aKeyevt[i].EvtArgs;
                string str = String.Format("{0} ({1})", kea.KeyCode, (int)kea.KeyCode);
                grfx.DrawString(str, Font, br, xCode, y);
                grfx.DrawString(kea.Modifiers.ToString(), Font, br, xMods, y);
                grfx.DrawString(kea.KeyData.ToString(), Font, br, xData, y);
                grfx.DrawString(kea.Shift.ToString(), Font, br, xShift, y);
                grfx.DrawString(kea.Control.ToString(), Font, br, xCtrl, y);
                grfx.DrawString(kea.Alt.ToString(), Font, br, xAlt, y);
            }
        }
    }
}