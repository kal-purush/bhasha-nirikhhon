package BusinessLayer.Components.UseCaseDiagramComponents;

import BusinessLayer.Components.UMLComponent;

import java.awt.*;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.awt.dnd.*;
import java.io.IOException;

public class UseCaseComponent extends UMLComponent implements DragGestureListener, DragSourceListener, Transferable {

    private static final DataFlavor USECASE_FLAVOR = new DataFlavor(UseCaseComponent.class, "UseCaseComponent");
    private static final DataFlavor[] SUPPORTED_FLAVORS = {USECASE_FLAVOR};

    private DragSource dragSource;

    public UseCaseComponent(){

        dragSource = DragSource.getDefaultDragSource();
        dragSource.createDefaultDragGestureRecognizer(
                this, // The Actor component itself
                DnDConstants.ACTION_COPY, // Allow copy operation
                this // Listener for drag gestures
        );
    }

    @Override
    public void draw(Graphics g) {

    }

    @Override
    public DataFlavor[] getTransferDataFlavors() {
        return SUPPORTED_FLAVORS;
    }

    @Override
    public boolean isDataFlavorSupported(DataFlavor flavor) {
        return flavor.equals(USECASE_FLAVOR);
    }

    @Override
    public Object getTransferData(DataFlavor flavor) throws UnsupportedFlavorException, IOException {
        if (flavor.equals(USECASE_FLAVOR)) {
            return this;
        } else {
            throw new UnsupportedFlavorException(flavor);
        }
    }

    @Override
    public void dragGestureRecognized(DragGestureEvent dge) {
        try {
            dge.startDrag(DragSource.DefaultCopyDrop, this, this);
        } catch (InvalidDnDOperationException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void dragEnter(DragSourceDragEvent dsde) {

    }

    @Override
    public void dragOver(DragSourceDragEvent dsde) {

    }

    @Override
    public void dropActionChanged(DragSourceDragEvent dsde) {

    }

    @Override
    public void dragExit(DragSourceEvent dse) {

    }

    @Override
    public void dragDropEnd(DragSourceDropEvent dsde) {

    }
}