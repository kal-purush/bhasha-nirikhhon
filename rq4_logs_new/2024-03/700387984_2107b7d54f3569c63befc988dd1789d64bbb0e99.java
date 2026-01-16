package carleton.sysc4907.command;

import carleton.sysc4907.command.args.*;
import carleton.sysc4907.model.DiagramModel;
import carleton.sysc4907.processing.ElementIdManager;

import java.util.*;
import java.util.stream.Collectors;

public class CommandListCompressor {

    private final DiagramModel diagramModel;
    private final ElementIdManager elementIdManager;

    // I wish we could find these via reflection but no
    private final Class[] commandArgsClasses = new Class[] {
            AddCommandArgs.class,
            RemoveCommandArgs.class,
            ConnectorMovePointCommandArgs.class,
            EditTextCommandArgs.class,
            MoveCommandArgs.class,
            RemoveCommandArgs.class,
            ResizeCommandArgs.class
    };

    public CommandListCompressor(DiagramModel diagramModel, ElementIdManager elementIdManager) {
        this.diagramModel = diagramModel;
        this.elementIdManager = elementIdManager;
    }

    public List<Command<?>> compressCommandList(List<Command<?>> commandList) {
        var cmdList = new LinkedList<>(commandList); // avoid race conditions: ignore any further commands added
        var indices = new HashSet<Integer>();
        var usedIds = elementIdManager.getUsedIds();
        for (Long id : usedIds) {
            var idCompressedList = compressCommandListForId(filterById(cmdList, id));
            indices.addAll(idCompressedList.stream().map(cmdList::indexOf).toList());
        }
        for (Integer i : indices) {
            System.out.print(i);
        }
        System.out.println();
        return indices.stream().map(cmdList::get).collect(Collectors.toList());
    }

    private List<Command<?>> filterById(List<Command<?>> commandList, Long id) {
        return commandList.stream().filter(
                cmd -> Arrays.stream(cmd.getArgs().getElementIds()).anyMatch(
                        id::equals
                )).toList();
    }

    private Command<?> findLastOfType(List<Command<?>> commandList, Class type) {
        var commandsOfType = commandList.stream().filter(cmd ->  type.isInstance(cmd.getArgs())).toList();
        if (commandsOfType.isEmpty()) {
            return null;
        } else {
            return commandsOfType.get(commandsOfType.size() - 1);
        }
    }

    private List<Command<?>> findLastConnectorMoves(List<Command<?>> commandList) {
        var movePointCommands = commandList.stream().filter(cmd ->  cmd.getArgs() instanceof ConnectorMovePointCommandArgs).toList();
        var startMoves = movePointCommands.stream().filter(cmd -> ((ConnectorMovePointCommandArgs) (cmd.getArgs())).isStart()).toList();
        var endMoves = movePointCommands.stream().filter(cmd -> !((ConnectorMovePointCommandArgs) (cmd.getArgs())).isStart()).toList();
        List<Command<?>> lastCommands = new LinkedList<>();
        if (!startMoves.isEmpty()) {
            lastCommands.add(startMoves.get(startMoves.size() - 1));
        }
        if (!endMoves.isEmpty()) {
            lastCommands.add(endMoves.get(endMoves.size() - 1));
        }
        return lastCommands;
    }

    private Command<?> findLastMove(List<Command<?>> commandList) {
        var moveCommands = commandList.stream().filter(this::isMove).toList();
        if (moveCommands.isEmpty()) {
            return null;
        } else {
            return moveCommands.get(moveCommands.size() - 1);
        }
    }

    private boolean isMove(Command<?> command) {
        var args = command.getArgs();
        // Move commands are moves
        if (args instanceof MoveCommandArgs) return true;
        // Resize commands cause a move for any corner except bottom right
        if (args instanceof ResizeCommandArgs) {
            var resizeArgs = (ResizeCommandArgs) args;
            if (resizeArgs.isTopAnchor() || !resizeArgs.isRightAnchor()) {
                return true;
            }
        }
        // Any other command is not a move
        return false;
    }

    private List<Command<?>> compressCommandListForId(List<Command<?>> idCommandList) {
        List<Command<?>> compressedList = new LinkedList<>();
        for (Class argsType : commandArgsClasses) {
            var lastCommandOfType = findLastOfType(idCommandList, argsType);
            if (lastCommandOfType != null) {
                compressedList.add(lastCommandOfType);
            }
        }
        var lastMove = findLastMove(idCommandList);
        if (lastMove != null) {
            compressedList.add(lastMove);
        }
        compressedList.addAll(findLastConnectorMoves(idCommandList));
        return compressedList;
    }
}