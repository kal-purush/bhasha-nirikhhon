using System;
using System.Collections.Generic;

namespace BattleSpace.Lib.Test;

public class DeleteProperty : Hwdtech.ICommand {
    IUObject obj; 
    IList<string> properties;
    IPropertyDelete propertyDelete;
    public DeleteProperty(IPropertyDelete propertyDelete, IUObject obj,  IList<string> properties){
        this.obj = obj;
        this.properties = properties;
        this.propertyDelete = propertyDelete;
    }
    public void Execute() {
        for (int i = 0; i < properties.Count; i++ ) {
            propertyDelete.DeleteProperty(obj ,properties[i]);
        }
    }
}