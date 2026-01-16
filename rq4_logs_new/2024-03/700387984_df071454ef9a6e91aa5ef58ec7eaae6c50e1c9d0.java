package carleton.sysc4907.command;

import carleton.sysc4907.command.args.ConnectorMovePointCommandArgs;
import carleton.sysc4907.command.args.ConnectorSnapCommandArgs;
import carleton.sysc4907.controller.element.ConnectorElementController;
import carleton.sysc4907.processing.ElementIdManager;
import carleton.sysc4907.view.SnapHandle;

public class ConnectorSnapCommand implements Command<ConnectorSnapCommandArgs> {

    private final ConnectorSnapCommandArgs args;
    private final ElementIdManager elementIdManager;

    public ConnectorSnapCommand(ConnectorSnapCommandArgs args, ElementIdManager elementIdManager) {
        this.args = args;
        this.elementIdManager = elementIdManager;
    }

    /**
     * Executes the command
     */
    @Override
    public void execute() {
        var connector = elementIdManager.getElementById(args.connectorElementId());
        var snapHandleNode = elementIdManager.getElementById(args.snapHandleId());
        if (connector == null || snapHandleNode == null) return;
        if (!(elementIdManager.getElementControllerById(args.connectorElementId())
                instanceof ConnectorElementController connectorElementController)) {
            System.out.println("Element with given ID for snapping was not a connector");
            return;
        }
        if (!(snapHandleNode instanceof SnapHandle snapHandle)) {
            System.out.println("Snap handle ID did not refer to a snap handle");
            return;
        }
        if (args.isUnsnap()) {
            connectorElementController.unsnapFromHandle(args.isStart());
        } else {
            connectorElementController.snapToHandle(snapHandle, args.isStart());
        }
    }

    @Override
    public ConnectorSnapCommandArgs getArgs() {
        return args;
    }
}