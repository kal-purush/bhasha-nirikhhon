package org.argouml.uml.diagram.ui;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.argouml.model.Model;
import org.tigris.gef.base.Globals;
import org.tigris.gef.base.Selection;
import org.tigris.gef.presentation.Fig;

public class ActionHelper {

    public static Object getSelectedAssociationEnd(String str) {
        List sels = Globals.curEditor().getSelectionManager().selections();
        if (sels.size() == 1) {
            Selection sel = (Selection) sels.get(0);
            Fig f = sel.getContent();
            Object owner = ((FigEdgeModelElement) f).getOwner();
            Collection ascEnds = Model.getFacade().getConnections(owner);
            Iterator iter = ascEnds.iterator();
            if (str.equals("src")) {
                return iter.next();
            } else {
                Object ascEnd = null;
                while (iter.hasNext()) {
                    ascEnd = iter.next();
                }
                return ascEnd;
            }
        }
        return null; // No valid selection
    }
}