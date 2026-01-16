package main.java.com.files;

import java.util.ArrayList;
import java.util.List;

public class Cell<T> {     
    
    List<Data<T>> intermediateValues = new ArrayList<Data<T>>();
    private int internalListPosition = -1; 
    
    public Cell(T origValue) {          
           changeToValue(origValue, true);           
    }   

    public void changeToValue(T newValue, boolean isValid) {
    	internalListPosition++;    	
    	makeAddedValueMostRecent();
    	intermediateValues.add(new Data<T>(newValue, isValid));    	
    }   
    
    public void changeToPreviousValue() {
    	if (internalListPosition!=0) {
    		internalListPosition--;
    	}    	
    }
    
    public void changeToNextValue() {
    	if (internalListPosition < intermediateValues.size() - 1) {
    		internalListPosition++;
    	}
    }
    
    public T getCurrentValue(){
    	Data<T> currentValue = null;
    	if (intermediateValues != null && !intermediateValues.isEmpty()) {
    		currentValue = intermediateValues.get(internalListPosition);
    	}
    	return currentValue.getValue();
    }
    
    public DataStatus getStatus() {
    	boolean isValid = intermediateValues.get(internalListPosition).getStatus();
    	boolean isChanged = isChanged();
    	DataStatus status;
    	if(isChanged&&isValid) {
    		status = DataStatus.CHANGED_OK;
    	} else if(isChanged&&!isValid) {
    		status = DataStatus.CHANGED_NOK;
    	} else if(!isChanged&&isValid) {
    		status = DataStatus.UNCHANGED_OK;
    	} else {
    		status = DataStatus.UNCHANGED_NOK;
    	}
    	return status;
    }
    
    public T getOriginalValue() {
    	return intermediateValues.get(0).getValue();
    }
    
    private void makeAddedValueMostRecent() {
    	intermediateValues = intermediateValues.subList(0, internalListPosition);
    }
    
    private boolean isChanged() {
    	T currentValue = getCurrentValue();
    	T origValue = intermediateValues.get(0).getValue();
    	return !(origValue.equals(currentValue) || origValue==currentValue);
    }
    


}