package jimmy.mcgymmy.logic.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import jimmy.mcgymmy.model.ModelManager;

/**
 * Contains unit tests for UndoCommand.
 */
public class UndoCommandTest {
    private class CanUndoModelStub extends ModelManager {
        @Override
        public boolean canUndo() {
            return true;
        }

        @Override
        public void undo() {
            // do nothing (to prevent EmptyStackException)
        }
    }

    private class CannotUndoModelStub extends ModelManager {
        @Override
        public boolean canUndo() {
            return false;
        }
    }

    @Test
    public void execute_canUndo_returnsUndoSuccessfulMessage() {
        UndoCommand command = new UndoCommand();
        CommandResult commandResult = command.execute(new CanUndoModelStub());
        assertEquals(new CommandResult(UndoCommand.MESSAGE_UNDO_SUCCESS), commandResult);
    }

    @Test
    public void execute_cannotUndo_returnsCannotUndoAnyMoreMessage() {
        UndoCommand command = new UndoCommand();
        CommandResult commandResult = command.execute(new CannotUndoModelStub());
        assertEquals(new CommandResult(UndoCommand.MESSAGE_NOT_UNDOABLE), commandResult);
    }
}