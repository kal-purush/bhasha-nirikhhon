using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Ezzen
{
    public class ChatGroup : Button
    {
        private string groupName;
        private string gid;

        public string GroupName { get => groupName; }
        public string Gid { get => gid; }


        public ChatGroup(string gName, string gID)
        {
            this.groupName = gName;
            this.gid = gID;
            this.FlatStyle = FlatStyle.Flat;
            this.FlatAppearance.BorderSize = 0;
            this.Font = new System.Drawing.Font("Tw Cen MT", 16F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Width = 270;
            this.Height = 50;
            this.Margin = new Padding(5, 5, 0, 0);
            this.ForeColor = System.Drawing.Color.Black;
            this.BackColor = System.Drawing.Color.WhiteSmoke;
            this.Text = groupName;
            this.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.MouseClick += chatGroup_MouseClick;
        }

        public void chatGroup_MouseClick(object sender, MouseEventArgs e)
        {
            Program.MW.GroupName1.Text = groupName;
            Program.MW.GroupID1.Text = gid;
        }
    }
}