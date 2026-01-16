package File_work.RTTI;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.Scanner;

public class task_3 {
    public static void main(String[] args) throws Exception {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("JavaScript");
        System.out.println("Print mathematics vars");
        String str = new Scanner(System.in).next();
        engine.eval("print(" + str + ")");
    }
}