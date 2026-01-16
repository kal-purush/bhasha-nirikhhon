using System;
using System.Collections.Generic;
using System.Drawing;
using System.Windows.Forms;

namespace BeGraph {
	/// <summary>
	/// Represents a user control for displaying directed graph.
	/// </summary>
	class GraphBox : PictureBox {

		private Graph g = new Graph();
		private Vertex last;

		public GraphBox() {
			
			// Binding event handlers to events.
			MouseDown += new MouseEventHandler(pb_MouseDown);
			MouseUp += new MouseEventHandler(pb_MouseUp);
			MouseDoubleClick += new MouseEventHandler(pb_DoubleClick);
			g.GraphChanged += new EventHandler(g_GraphChanged);
		}

		/// <summary>
		/// Checking if there is enought place at the mouse 
		/// cursor to create a vertex in GraphBox
		/// </summary>
		/// <param name="cursor"></param>
		/// <returns></returns>
		private bool haveSpace(Point cursor) {
			int r = Vertex.r;
			Point rbBorder = new Point(base.Width - r, base.Height - r);
			if (!((cursor.X > r && cursor.X < rbBorder.X) && (cursor.Y > r && cursor.Y < rbBorder.Y)))
				return false;
			return true;
		}

		/// <summary>
		/// Generating vertex at cursor point. Display input 
		/// form which requires you to enter name of the vertex. 
		/// </summary>
		/// <param name="p">p is current position of the mouse cusror</param>
		/// <returns>Returns generated vertex</returns>
		private Vertex generateVert(Point p) {
			if (haveSpace(p)) {
				InputDialog.InputDialog id = new InputDialog.InputDialog("Enter name of a vertex", "Vertex generation");
				DialogResult dResult = id.ShowDialog(this);
				if (dResult == DialogResult.OK) {
					Vertex v = new Vertex(id.getInput(), p);			
					return v;
				}
			}
			else
				MessageBox.Show("Invalid place to put a vertex!", "Error", MessageBoxButtons.OK);
			return null;
		}

		/// <summary>
		/// Overriding of the paint event for GraphBox
		/// </summary>
		/// <param name="pe"></param>
		protected override void OnPaint(PaintEventArgs pe) {
			base.OnPaint(pe);
			Pen p = new Pen(Color.Black);
			g.draw(pe.Graphics);
		}

		#region Events


		private void g_GraphChanged(object sender, EventArgs e) {
			((Graph)sender).draw(CreateGraphics());
		}

		private void pb_MouseDown(object sender, MouseEventArgs me) {
			switch (me.Button) {
				case MouseButtons.Left:
					if (me.Clicks == 1)
						last = g.vertAt(me.Location);
					break;
			}
		}

		private void pb_MouseUp(object sender, MouseEventArgs me) {
			switch (me.Button) {
				case MouseButtons.Left:
					if (me.Clicks == 1) {
						Vertex tempSecond = g.vertAt(me.Location);
						if (last != null && tempSecond != null) {
							Edge e = new Edge(last, tempSecond);
							g += e;
						}
					}
					break;
			}
		}

		private void pb_DoubleClick(object sender, EventArgs e) {
			MouseEventArgs me = (MouseEventArgs)e;
			if (g.vertAt(me.Location) == null) {
				Vertex v = generateVert(me.Location);
				if (v != null)
					g += v;
			}
		}
		#endregion
	}
}