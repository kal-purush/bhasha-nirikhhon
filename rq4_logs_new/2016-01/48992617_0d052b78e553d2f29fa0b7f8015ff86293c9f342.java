package grid.modification.elements;

import grid.entities.Grid;
import grid.entities.GridElement;

public class ListAppend extends GridElementModification {

	private String 	listNameToBeChanged;
	private String	appendedObjectLabel;
	
	public String getSubjectLabel() {
		return subjectLabel;
	}

	public void setSubjectLabel(String subjectLabel) {
		this.subjectLabel = subjectLabel;
	}

	public String getListNameToBeChanged() {
		return listNameToBeChanged;
	}

	public void setListNameToBeChanged(String listNameToBeChanged) {
		this.listNameToBeChanged = listNameToBeChanged;
	}

	public String getAppendedObjectLabel() {
		return appendedObjectLabel;
	}

	public void setAppendedObjectLabel(String appendedObjectLabel) {
		this.appendedObjectLabel = appendedObjectLabel;
	}

	@Override
	public void apply(GridElement anElement, Grid grid) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public String toString() {
		return "ListAppend: append this element "+this.appendedObjectLabel+"in this list "+this.listNameToBeChanged+" in this object "+this.subjectLabel;
	}

}