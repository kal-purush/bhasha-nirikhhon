package expression;

import antlr.SNBBaseListener;
import antlr.SNBParser;
import views.MainApplication;

public class AntlrToTracer extends SNBBaseListener {
    @Override
    public void enterProgram(SNBParser.ProgramContext ctx) {
        if (MainApplication.debugCalled) {
            MainApplication.debugResultsTextArea.append("Successfully entered program node \n");
        }
    }

    @Override
    public void exitProgram(SNBParser.ProgramContext ctx) {
        if (MainApplication.debugCalled) {
            MainApplication.debugResultsTextArea.append("Successfully exited program node \n");
        }
    }
}