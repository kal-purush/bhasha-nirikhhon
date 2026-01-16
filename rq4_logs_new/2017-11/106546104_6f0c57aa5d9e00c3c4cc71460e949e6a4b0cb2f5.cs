using System;
using Gtk;

namespace VRZKazerneInfo
{
    public class NewsMessageView : TreeView
    {

        private TreeViewColumn authorColumn, messageColumn, dateColumn;
        public ListStore newsMessageListStore { get; }

        public NewsMessageView ()
        {
            this.authorColumn = new TreeViewColumn ();
            authorColumn.Title = "Auteur";
            CellRendererText authorCell = new CellRendererText ();
            authorCell.BackgroundGdk = new Gdk.Color (0, 0, 0);
            this.authorColumn.PackStart (authorCell, true);
            this.authorColumn.AddAttribute (authorCell, "text", 0);

            this.dateColumn = new TreeViewColumn ();
            dateColumn.Title = "Datum";
            CellRendererText dateCell = new Gtk.CellRendererText ();
            dateCell.BackgroundGdk = new Gdk.Color (0, 0, 0);
            this.dateColumn.PackStart (dateCell, true);
            this.dateColumn.AddAttribute (dateCell, "text", 1);

            this.messageColumn = new TreeViewColumn ();
            messageColumn.Title = "Bericht";
            CellRendererText messageCell = new CellRendererText ();
            messageCell.BackgroundGdk = new Gdk.Color (0, 0, 0);
            this.messageColumn.PackStart (messageCell, true);
            this.messageColumn.AddAttribute (messageCell, "text", 2);

            this.AppendColumn (authorColumn);
            this.AppendColumn (dateColumn);
            this.AppendColumn (messageColumn);
            this.Sensitive = false;
            newsMessageListStore = new ListStore (typeof (string), typeof (string), typeof(string));
            this.Model = newsMessageListStore;
        }
    }
}