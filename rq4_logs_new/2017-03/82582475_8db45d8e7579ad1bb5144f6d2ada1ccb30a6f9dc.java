package com.foc.vaadin.gui.xmlForm;

import com.foc.Globals;
import com.foc.desc.FocConstructor;
import com.foc.desc.FocDesc;
import com.foc.desc.FocObject;
import com.foc.shared.xmlView.XMLViewKey;
import com.foc.vaadin.ICentralPanel;
import com.foc.vaadin.gui.components.ITableTree;
import com.foc.vaadin.gui.layouts.validationLayout.FVValidationLayout;
import com.foc.web.server.xmlViewDictionary.XMLViewDictionary;

@SuppressWarnings("serial")
public abstract class FocXMLLayout_JoinTable extends FocXMLLayout {
	
	abstract public FocDesc getOriginalFocDesc();
	abstract public int getOriginalObjectReference(FocObject focObject);
	
	@Override
	public ICentralPanel table_OpenItem(String tableName, ITableTree table, FocObject focObject, int viewContainer_Open) {
		FocConstructor constr = new FocConstructor(getOriginalFocDesc(), null);
		FocObject newObject = (FocObject) constr.newItem();
		newObject.setReference(getOriginalObjectReference(focObject));
		newObject.load();

		XMLViewKey xmlViewKey = new XMLViewKey(newObject.getThisFocDesc().getStorageName(), XMLViewKey.TYPE_FORM);
		ICentralPanel centralPanel = XMLViewDictionary.getInstance().newCentralPanel(getMainWindow(), xmlViewKey, newObject);
		centralPanel.setFocDataOwner(true);
		getMainWindow().changeCentralPanelContent(centralPanel, true);
		
		return centralPanel;
	}
	
	@Override
	public FocObject table_AddItem(String tableName, ITableTree table, FocObject fatherObject) {
		FocConstructor constr = new FocConstructor(getOriginalFocDesc(), null);
		FocObject incident = (FocObject) constr.newItem();
		incident.setCreated(true);
		incident.setCompany(Globals.getApp().getCurrentCompany());
		
		XMLViewKey xmlViewKey = new XMLViewKey(incident.getThisFocDesc().getStorageName(), XMLViewKey.TYPE_FORM);
		ICentralPanel centralPanel = XMLViewDictionary.getInstance().newCentralPanel(getMainWindow(), xmlViewKey, incident);
		getMainWindow().changeCentralPanelContent(centralPanel, true);
		
		centralPanel.getValidationLayout().addValidationListener(new IValidationListener() {
			@Override
			public void validationDiscard(FVValidationLayout validationLayout) {
			}
			
			@Override
			public boolean validationCommit(FVValidationLayout validationLayout) {
				return false;
			}
			
			@Override
			public void validationAfter(FVValidationLayout validationLayout, boolean commited) {
				if(getFocList() != null){
					getFocList().reloadFromDB();
				}
			}
		});
		
		return incident;
	}
	
	@Override
	public void table_DeleteItem(ITableTree table, FocObject focObject) {
		FocConstructor constr = new FocConstructor(getOriginalFocDesc(), null);
		FocObject newObject = (FocObject) constr.newItem();
		newObject.setReference(getOriginalObjectReference(focObject));
		newObject.load();
		newObject.setDeleted(true);
		boolean error = !newObject.validate(true);

		if(!error){
			super.table_DeleteItem(table, focObject);
		}
	}
	
}