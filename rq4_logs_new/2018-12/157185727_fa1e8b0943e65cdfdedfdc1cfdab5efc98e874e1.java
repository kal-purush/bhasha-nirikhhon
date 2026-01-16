package de.ovgu.mb.iaf.model.adapter.AMLTree;

import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import javax.swing.tree.TreeNode;

import de.ovgu.mb.iaf.data.caex.InternalElementType;
import de.ovgu.mb.iaf.model.application.ITreeNodeAdapter;

public class InternalElementTreeAdapter implements ITreeNodeAdapter {
	InternalElementType wrappedelement;

	public InternalElementTreeAdapter(InternalElementType element) {
		this.wrappedelement = element;

	}

	@Override
	public Enumeration children() {
		
		
		Vector<Object> list = new Vector<>();
		list.addAll(wrappedelement.getInternalElement());
		list.addAll(wrappedelement.getSupportedRoleClass());
		
		return list.elements();
	}

	@Override
	public boolean getAllowsChildren() {
		return true;
	}

	@Override
	public TreeNode getChildAt(int arg0) {
		
		
		System.out.println("InternalElementAdapter arg0: " + arg0 + "Gesamtzahl: "+ getChildCount());
		if (arg0 > (wrappedelement.getInternalElement().size() - 1)) {
			
			System.out.println("InternlElementAdapter size: " + wrappedelement.getInternalElement().size() );
			System.out.println("InternlElementAdapter role classsize: " + wrappedelement.getSupportedRoleClass().size());

			return (TreeNode) amlTreeAdapterFactory.getInstance().getAdapter(
					wrappedelement.getSupportedRoleClass().get(arg0 - wrappedelement.getInternalElement().size()),
					TreeNode.class);

		} else {

			return (TreeNode) amlTreeAdapterFactory.getInstance()
					.getAdapter(wrappedelement.getInternalElement().get(arg0), TreeNode.class);

		}

	}

	@Override
	public int getChildCount() {
		
		return wrappedelement.getInternalElement().size() + wrappedelement.getSupportedRoleClass().size();

	}

	@Override
	public int getIndex(TreeNode arg0) {
		int x = -1;

		if (arg0 instanceof ITreeNodeAdapter) {

			ITreeNodeAdapter myadapter = (ITreeNodeAdapter) arg0;
			x = wrappedelement.getInternalElement().indexOf(myadapter.getInternalObject());

			if (x == -1) {
				x = wrappedelement.getInternalElement().size()
						+ wrappedelement.getSupportedRoleClass().indexOf(myadapter.getInternalObject());
			}
		}

		return x;
	}

	@Override
	public TreeNode getParent() {
		return (TreeNode) amlTreeAdapterFactory.getInstance().getAdapter(wrappedelement.getParent(), TreeNode.class);

	}

	@Override // last Element?
	public boolean isLeaf() {
		
		if(getChildCount() <= 0)
			return true;

		return false;
	}

	@Override
	public Object getInternalObject() {

		return wrappedelement;
	}

	@Override
	public String getLabel() {

		return wrappedelement.getName() + ", " + wrappedelement.getID();
	}
	
	public String toString() {

		return wrappedelement.getName() + ", " + wrappedelement.getID();
	}

}