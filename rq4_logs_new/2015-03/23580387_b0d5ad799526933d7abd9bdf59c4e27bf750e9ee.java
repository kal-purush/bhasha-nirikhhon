package info.ccserver.homunculus.engine.javascript;

import info.ccserver.homunculus.engine.IEngine;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class JavaScriptEngine implements IEngine {

	private static ScriptEngine engine;

	public JavaScriptEngine() throws ScriptException {
		// TODO 自動生成されたコンストラクター・スタブ
		engine = new ScriptEngineManager().getEngineByName("JavaScript");
		String engineName = engine.getFactory().getEngineName();
		switch (engineName) {
		case "Oracle Nashorn":
			engine.eval("load('nashorn:mozilla_compat.js');");
			System.out.println("mozilla_compat loaded.");
			break;

		default:
			break;
		}
		System.out.println();
		// System.out.println("engine = " + engine);
	}

	@Override
	public Object eval(String script) {
		try {
			return engine.eval(script);
		} catch (ScriptException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
			return null;
		}

	}
}