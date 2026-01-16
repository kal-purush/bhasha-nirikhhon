using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Windows.Forms;

namespace KindleLibrarySynchronizer
{
	public class MultiSelectTreeView : TreeView
	{
		public event EventHandler SelectionChanged;

		private const int TVS_NOSCROLL = 0x2000;
		private const int TVS_NOHSCROLL = 0x8000;

		private TreeNode selectedNode;
		private List<TreeNode> selectedNodes;


		// Note we use the new keyword to hide the native TreeView.SelectedNode property.
		public new TreeNode SelectedNode
		{
			get
			{
				return selectedNode;
			}
			set
			{
				ClearSelectedNodes();
				if (value != null)
				{
					SelectNode(value);
				}
			}
		}

		public List<TreeNode> SelectedNodes
		{
			get
			{
				return selectedNodes;
			}
			/*set
			{
				ClearSelectedNodes();
				if (value != null)
				{
					foreach (TreeNode node in value)
					{
						ToggleNode(node, true);
					}
				}
			}/**/
		}


		public MultiSelectTreeView()
		{
			selectedNode = null;
			selectedNodes = new List<TreeNode>();
			base.SelectedNode = null;
		}

		protected override CreateParams CreateParams
		{
			get
			{
				CreateParams cp = base.CreateParams;
				cp.Style |= TVS_NOHSCROLL;
				return cp;
			}
		}


		protected override void OnGotFocus(EventArgs e)
		{
			//Console.Out.WriteLine("OnGotFocus");

			try
			{
				// Make sure at least one node has a selection
				// this way we can tab to the ctrl and use the 
				// keyboard to select nodes
				/*if (m_SelectedNode == null && this.TopNode != null)
				{
					ToggleNode( this.TopNode, true );
				}/**/

				UpdateNodesColor();

				base.OnGotFocus(e);
			}
			catch (Exception ex)
			{
				HandleException(ex);
			}
		}

		protected override void OnLostFocus(EventArgs e)
		{
			//Console.Out.WriteLine("OnLostFocus");

			try
			{
				UpdateNodesColor();

				base.OnLostFocus(e);
			}
			catch (Exception ex)
			{
				HandleException(ex);
			}
		}

		protected override void OnMouseDown(MouseEventArgs e)
		{
			//Console.Out.WriteLine("OnMouseDown");

			base.OnMouseDown(e);

			// If the user clicks on a node that was not previously selected, select it now.
			try
			{
				TreeNode node = this.GetNodeAt(e.Location);
				if (node != null)
				{
					int leftBound = node.Bounds.X; // - 20; // Allow user to click on image
					int rightBound = node.Bounds.Right + 10; // Give a little extra room
					if (e.Location.X < leftBound)
					{
						// Click on lines area.
						// Nothing to do here.
					}
					else if (e.Location.X > rightBound)
					{
						// Click at the blank space. Clear selection.
						ClearSelectedNodes();
					}
					else if (ModifierKeys == Keys.None && (selectedNodes.Contains(node)))
					{
						// Potential Drag Operation
						// Let MouseUp do select
					}
					else
					{
						// Click at node. Select.
						SelectNode(node);
					}
				}
				else
				{
					// Click below the nodes. Clear selection.
					ClearSelectedNodes();
				}

				base.SelectedNode = null;
			}
			catch (Exception ex)
			{
				HandleException(ex);
			}
		}

		protected override void OnMouseUp(MouseEventArgs e)
		{
			//Console.Out.WriteLine("OnMouseUp");

			// If clicked on a node that WAS previously selected then, reselect it now.
			// This will clear any other selected nodes. E.g. A B C D are selected
			// and the user clicks on B, now A C & D are not longer selected.
			try
			{
				// Check to see if a node was clicked on 
				TreeNode node = this.GetNodeAt(e.Location);
				if (node != null)
				{
					if (ModifierKeys == Keys.None && selectedNodes.Contains(node))
					{
						int leftBound = node.Bounds.X; // -20; // Allow user to click on image
						int rightBound = node.Bounds.Right + 10; // Give a little extra room
						if (e.Location.X > leftBound &&
							e.Location.X < rightBound &&
							e.Button == MouseButtons.Left)
						{
							SelectNode(node);
						}
					}
				}

				base.OnMouseUp(e);
			}
			catch (Exception ex)
			{
				HandleException(ex);
			}
		}

		protected override void OnItemDrag(ItemDragEventArgs e)
		{
			// If the user drags a node and the node being dragged is NOT
			// selected, then clear the active selection, select the
			// node being dragged and drag it. Otherwise if the node being
			// dragged is selected, drag the entire selection.
			try
			{
				TreeNode node = e.Item as TreeNode;

				if (node != null && !selectedNodes.Contains(node))
				{
					SelectSingleNode(node);
					ToggleNode(node, true);
				}

				base.OnItemDrag(e);
			}
			catch (Exception ex)
			{
				HandleException(ex);
			}
		}

		protected override void OnBeforeSelect(TreeViewCancelEventArgs e)
		{
			//Console.Out.WriteLine("OnBeforeSelect: " + (e.Node == null ? "null" : e.Node.Text));

			// Never allow base.SelectedNode to be set!
			try
			{
				base.SelectedNode = null;
				e.Cancel = true;

				base.OnBeforeSelect(e);
			}
			catch (Exception ex)
			{
				HandleException(ex);
			}
			base.OnBeforeSelect(e);
		}

		protected override void OnAfterSelect(TreeViewEventArgs e)
		{
			//Console.Out.WriteLine("OnAfterSelect: " + (e.Node == null ? "null" : e.Node.Text));

			// Never allow base.SelectedNode to be set!
			try
			{
				base.OnAfterSelect(e);
				base.SelectedNode = null;
			}
			catch (Exception ex)
			{
				HandleException(ex);
			}
			base.OnAfterSelect(e);
		}

		protected override void OnKeyDown(KeyEventArgs e)
		{
			// Handle all possible key strokes for the control.
			// including navigation, selection, etc.

			base.OnKeyDown(e);

			if (e.KeyCode == Keys.ShiftKey) return;
			if (e.KeyCode == Keys.ControlKey) return;

			//this.BeginUpdate();
			bool bShift = (ModifierKeys == Keys.Shift);

			try
			{
				// Nothing is selected in the tree, this isn't a good state
				// select the top node
				if (selectedNode == null && this.TopNode != null)
				{
					ToggleNode(this.TopNode, true);
				}

				// Nothing is still selected in the tree, this isn't a good state, leave.
				if (selectedNode == null) return;


				switch (e.KeyData)
				{
					case Keys.Left:
						if (selectedNode.IsExpanded && selectedNode.Nodes.Count > 0)
						{
							// Collapse an expanded node that has children
							selectedNode.Collapse();
						}
						else if (selectedNode.Parent != null)
						{
							// Node is already collapsed, try to select its parent.
							SelectSingleNode(selectedNode.Parent);
						}
						break;

					case Keys.Right:
						if (!selectedNode.IsExpanded)
						{
							// Expand a collapsed node's children
							selectedNode.Expand();
						}
						else
						{
							// Node was already expanded, select the first child
							SelectSingleNode(selectedNode.FirstNode);
						}
						break;

					case Keys.Up:
						// Select the previous node
						if (selectedNode.PrevVisibleNode != null)
						{
							SelectNode(selectedNode.PrevVisibleNode);
						}
						break;

					case Keys.Down:
						// Select the next node
						if (selectedNode.NextVisibleNode != null)
						{
							SelectNode(selectedNode.NextVisibleNode);
						}
						break;

					case Keys.Home:
						if (bShift)
						{
							if (selectedNode.Parent == null)
							{
								// Select all of the root nodes up to this point 
								if (this.Nodes.Count > 0)
								{
									SelectNode(this.Nodes[0]);
								}
							}
							else
							{
								// Select all of the nodes up to this point under this nodes parent
								SelectNode(selectedNode.Parent.FirstNode);
							}
						}
						else
						{
							// Select this first node in the tree
							if (this.Nodes.Count > 0)
							{
								SelectSingleNode(this.Nodes[0]);
							}
						}
						break;

					case Keys.End:
						if (bShift)
						{
							if (selectedNode.Parent == null)
							{
								// Select the last ROOT node in the tree
								if (this.Nodes.Count > 0)
								{
									SelectNode(this.Nodes[this.Nodes.Count - 1]);
								}
							}
							else
							{
								// Select the last node in this branch
								SelectNode(selectedNode.Parent.LastNode);
							}
						}
						else
						{
							if (this.Nodes.Count > 0)
							{
								// Select the last node visible node in the tree.
								// Don't expand branches incase the tree is virtual
								TreeNode ndLast = this.Nodes[this.Nodes.Count - 1].LastNode;
								while (ndLast.IsExpanded && (ndLast.LastNode != null))
								{
									ndLast = ndLast.LastNode;
								}
								SelectSingleNode(ndLast);
							}
						}
						break;

					case Keys.PageUp:
						{
							// Select the highest node in the display
							int nCount = this.VisibleCount;
							TreeNode ndCurrent = selectedNode;
							while ((nCount) > 0 && (ndCurrent.PrevVisibleNode != null))
							{
								ndCurrent = ndCurrent.PrevVisibleNode;
								nCount--;
							}
							SelectSingleNode(ndCurrent);
						}
						break;

					case Keys.PageDown:
						{
							// Select the lowest node in the display
							int nCount = this.VisibleCount;
							TreeNode ndCurrent = selectedNode;
							while ((nCount) > 0 && (ndCurrent.NextVisibleNode != null))
							{
								ndCurrent = ndCurrent.NextVisibleNode;
								nCount--;
							}
							SelectSingleNode(ndCurrent);
						}
						break;

					case Keys.A | Keys.Control:
						{
							// Select All (CTRL-A), selects all top level nodes
							this.ClearSelectedNodes();
							this.CollapseAll();
							TreeNode ndCurrent = this.TopNode;
							while (ndCurrent != null)
							{
								ToggleNode(ndCurrent, true);
								ndCurrent = ndCurrent.NextNode;
							}
						}
						break;

					default:
						{
							// Assume this is a search character a-z, A-Z, 0-9, etc.
							// Select the first node after the current node that 
							// starts with this character
							string sSearch = ((char)e.KeyValue).ToString();

							TreeNode ndCurrent = selectedNode;
							while ((ndCurrent.NextVisibleNode != null))
							{
								ndCurrent = ndCurrent.NextVisibleNode;
								if (ndCurrent.Text.StartsWith(sSearch))
								{
									SelectSingleNode(ndCurrent);
									break;
								}
							}
						}
						break;
				} // switch
			}
			catch (Exception ex)
			{
				HandleException(ex);
			}
			finally
			{
				this.EndUpdate();
			}
		}

		protected override void OnDrawNode(DrawTreeNodeEventArgs e)
		{
			base.OnDrawNode(e);

			/*if (SelectedNodes.Contains(e.Node))
			{
				Pen pen = new Pen(e.Node.BackColor);
				pen.DashStyle = DashStyle.Dot;

				e.Graphics.FillRectangle(new SolidBrush(e.Node.BackColor), e.Node.Bounds);
				e.Graphics.DrawRectangle(pen, e.Node.Bounds);
				e.Graphics.DrawString(e.Node.Text, Font, SystemBrushes.HighlightText, e.Bounds.Location);
			}
			else
			{
				e.Graphics.DrawString(e.Node.Text, Font, new SolidBrush(e.Node.ForeColor), e.Bounds.Location);
			}/**/
		}


		private void SelectNode(TreeNode node)
		{
			try
			{
				this.BeginUpdate();

				if (selectedNode == null || ModifierKeys == Keys.Control)
				{
					// Ctrl+Click selects an unselected node, or unselects a selected node.
					bool bIsSelected = selectedNodes.Contains(node);
					ToggleNode(node, !bIsSelected);
				}
				else if (ModifierKeys == Keys.Shift)
				{
					// Shift+Click selects nodes between the selected node and here.
					TreeNode ndStart = selectedNode;
					TreeNode ndEnd = node;

					if (ndStart.Parent == ndEnd.Parent)
					{
						// Selected node and clicked node have same parent, easy case.
						if (ndStart.Index < ndEnd.Index)
						{
							// If the selected node is beneath the clicked node walk down
							// selecting each Visible node until we reach the end.
							while (ndStart != ndEnd)
							{
								ndStart = ndStart.NextVisibleNode;
								if (ndStart == null) break;
								ToggleNode(ndStart, true);
							}
						}
						else if (ndStart.Index == ndEnd.Index)
						{
							// Clicked same node, do nothing
						}
						else
						{
							// If the selected node is above the clicked node walk up
							// selecting each Visible node until we reach the end.
							while (ndStart != ndEnd)
							{
								ndStart = ndStart.PrevVisibleNode;
								if (ndStart == null) break;
								ToggleNode(ndStart, true);
							}
						}
					}
					else
					{
						// Selected node and clicked node have same parent, hard case.
						// We need to find a common parent to determine if we need
						// to walk down selecting, or walk up selecting.

						TreeNode ndStartP = ndStart;
						TreeNode ndEndP = ndEnd;
						int startDepth = Math.Min(ndStartP.Level, ndEndP.Level);

						// Bring lower node up to common depth
						while (ndStartP.Level > startDepth)
						{
							ndStartP = ndStartP.Parent;
						}

						// Bring lower node up to common depth
						while (ndEndP.Level > startDepth)
						{
							ndEndP = ndEndP.Parent;
						}

						// Walk up the tree until we find the common parent
						while (ndStartP.Parent != ndEndP.Parent)
						{
							ndStartP = ndStartP.Parent;
							ndEndP = ndEndP.Parent;
						}

						// Select the node
						if (ndStartP.Index < ndEndP.Index)
						{
							// If the selected node is beneath the clicked node walk down
							// selecting each Visible node until we reach the end.
							while (ndStart != ndEnd)
							{
								ndStart = ndStart.NextVisibleNode;
								if (ndStart == null) break;
								ToggleNode(ndStart, true);
							}
						}
						else if (ndStartP.Index == ndEndP.Index)
						{
							if (ndStart.Level < ndEnd.Level)
							{
								while (ndStart != ndEnd)
								{
									ndStart = ndStart.NextVisibleNode;
									if (ndStart == null) break;
									ToggleNode(ndStart, true);
								}
							}
							else
							{
								while (ndStart != ndEnd)
								{
									ndStart = ndStart.PrevVisibleNode;
									if (ndStart == null) break;
									ToggleNode(ndStart, true);
								}
							}
						}
						else
						{
							// If the selected node is above the clicked node walk up
							// selecting each Visible node until we reach the end.
							while (ndStart != ndEnd)
							{
								ndStart = ndStart.PrevVisibleNode;
								if (ndStart == null) break;
								ToggleNode(ndStart, true);
							}
						}
					}
				}
				else
				{
					// Just clicked a node, select it
					SelectSingleNode(node);
				}

				OnAfterSelect(new TreeViewEventArgs(selectedNode));
			}
			finally
			{
				this.EndUpdate();
			}
		}

		public void ClearSelectedNodes()
		{
			selectedNodes.Clear();
			selectedNode = null;

			RaiseSelectionChanged();

			UpdateNodesColor();
		}

		private void SelectSingleNode(TreeNode node)
		{
			if (node == null)
			{
				return;
			}

			ClearSelectedNodes();
			ToggleNode(node, true);
			node.EnsureVisible();
		}

		private void ToggleNode(TreeNode node, bool bSelectNode)
		{
			if (bSelectNode)
			{
				selectedNode = node;

				if (!selectedNodes.Contains(node))
				{
					selectedNodes.Add(node);
					RaiseSelectionChanged();
				}
			}
			else
			{
				if (selectedNodes.Contains(node))
				{
					selectedNodes.Remove(node);
					RaiseSelectionChanged();
				}
			}

			UpdateNodeColor(node, selectedNodes.Contains(node), Focused, HideSelection);
		}

		private void HandleException(Exception ex)
		{
			// Perform some error handling here.
			// We don't want to bubble errors to the CLR. 
			MessageBox.Show(ex.Message);
		}

		private void RaiseSelectionChanged()
		{
			if (SelectionChanged != null)
			{
				SelectionChanged(this, EventArgs.Empty);
			}
		}


		public void UpdateNodesColor()
		{
			BeginUpdate();
			UpdateNodesColor(Nodes);
			EndUpdate();
		}

		private void UpdateNodesColor(TreeNodeCollection nodes)
		{
			foreach (TreeNode node in nodes)
			{
				UpdateNodeColor(node, selectedNodes.Contains(node), Focused, HideSelection);
				if (node.Nodes.Count > 0)
				{
					UpdateNodesColor(node.Nodes);
				}
			}
		}

		private static void UpdateNodeColor(TreeNode node, bool nodeSelected, bool treeFocused, bool hideSelection)
		{
			/*if (!nodeSelected)
			{
				node.BackColor = SystemColors.Window;
				node.ForeColor = node.Tag == null ? SystemColors.WindowText;
			}
			else if (treeFocused)
			{
				node.BackColor = SystemColors.Highlight;
				node.ForeColor = SystemColors.HighlightText;
			}
			else if (hideSelection)
			{
				node.BackColor = SystemColors.Window;
				node.ForeColor = node.Tag == null ? SystemColors.WindowText;
			}
			else
			{
				node.BackColor = SystemColors.Control;
				node.ForeColor = node.Tag == null ? SystemColors.WindowText;
			}/**/
		}
	}
}