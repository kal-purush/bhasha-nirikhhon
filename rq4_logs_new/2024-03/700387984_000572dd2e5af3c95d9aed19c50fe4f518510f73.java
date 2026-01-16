package carleton.sysc4907.command;

import carleton.sysc4907.command.args.ChangeConnectorStyleCommandArgs;
import carleton.sysc4907.controller.element.ArrowConnectorElementController;
import carleton.sysc4907.processing.ElementIdManager;

public class ChangeConnectorStyleCommand implements Command<ChangeConnectorStyleCommandArgs> {

    private final ChangeConnectorStyleCommandArgs args;
    private final ElementIdManager elementIdManager;

    public ChangeConnectorStyleCommand(ChangeConnectorStyleCommandArgs args, ElementIdManager elementIdManager) {
        this.args = args;
        this.elementIdManager = elementIdManager;
    }

    /**
     * Executes the command
     */
    @Override
    public void execute() {
        var controller = elementIdManager.getElementControllerById(args.elementId());
        if (controller == null) return;
        if (controller instanceof ArrowConnectorElementController arrowConnectorElementController) {
            arrowConnectorElementController.setArrowheadType(args.type());
            arrowConnectorElementController.setPathStyle(args.type());
        }
    }

    @Override
    public ChangeConnectorStyleCommandArgs getArgs() {
        return args;
    }
}