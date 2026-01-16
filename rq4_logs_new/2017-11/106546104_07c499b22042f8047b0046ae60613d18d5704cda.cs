using System;
using System.Threading;
using Gtk;
using Pango;

public partial class MainWindow : Gtk.Window
{
	private Label clockLabel;

	public MainWindow(string title) : base(title)
	{
		var label = new Label("Vandaag prachtig weer");

		var vbox = new VBox();
		base.ModifyBg(StateType.Normal, new Gdk.Color(0, 0, 0));
		this.clockLabel = new Label(DateTime.Now.ToString("HH:mm:ss"));
		this.clockLabel.ModifyFg (StateType.Normal, new Gdk.Color(12, 237, 4));
		label.ModifyFg(StateType.Normal, new Gdk.Color(12, 237, 4));
		FontDescription fontDescription = new FontDescription();
		fontDescription.Size = Convert.ToInt32 (74.0 * Pango.Scale.PangoScale);
		this.clockLabel.ModifyFont (fontDescription);
		this.clockLabel.Justify = Justification.Fill;
		vbox.Add (this.clockLabel);
		var timer = new Timer(delegate (object state) {
			this.clockLabel.Text = DateTime.Now.ToString("HH:mm:ss");
		}, null, 0, 1000);

		this.clockLabel.Show ();

		vbox.Add(label);
		label.Show();
		this.Add(vbox);
		vbox.Show();
	}

	protected void OnDeleteEvent(object sender, DeleteEventArgs a)
	{
		Application.Quit();
		a.RetVal = true;
	}
}
