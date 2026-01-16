using System;
using System.Threading;
using Gtk;
using Pango;

namespace VRZKazerneInfo
{
    /// <summary>
    /// Simple label for showing the time
    /// </summary>
    public class ClockLabel : Gtk.Label
    {
        private string text;

        public ClockLabel()
        {
            this.Text = DateTime.Now.ToString("HH:mm:ss");
            this.initializeStyle();
            this.initializeTimer();
        }

        private void initializeStyle()
        {
            this.ModifyFg(StateType.Normal, new Gdk.Color(12, 237, 4));
            FontDescription fontDescription = new FontDescription();
            fontDescription.Size = Convert.ToInt32 (74.0 * Pango.Scale.PangoScale);
            this.ModifyFont (fontDescription);
            this.Justify = Justification.Fill;
        }

        private void initializeTimer()
        {
            new Timer(delegate (object state) {
                this.Text = DateTime.Now.ToString("HH:mm:ss");
            }, null, 0, 1000);
        }
    }
}
