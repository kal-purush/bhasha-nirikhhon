package com.dujay.jvm_language;

import org.antlr.v4.runtime.ParserRuleContext;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import com.dujay.jvm_language.args.CommandLineArgs;
import com.dujay.jvm_language.codegen.CodeGenerator;
import com.dujay.jvm_language.parser.JvmLanguageParser;
import com.dujay.jvm_language.utils.Either;

/**
 *
 */
public class Compiler 
{
  private CommandLineArgs args;
  public Compiler(String[] argv) {
    args = new CommandLineArgs();
    args.parse(argv);
  }
  /**
   * Parse the input file and return an either.
   * @param file
   * @return
   */
  public Either<ParserRuleContext, Exception> parse(String file) {
    JvmLanguageParser parser = new JvmLanguageParser();
    return parser.parse(file);
  }
  
  public Either<Void,Exception> semantics(ParserRuleContext p) {
    return Either.left(null);
  }
  public Either<Void,Exception> generate(Void v) {
    CodeGenerator generator = new CodeGenerator();
    return generator.generate();
  }
  public Void logError(Exception e) {
    System.out.println(e);
    return null;
  }
  
  /**
   * Run on all the arguments passed in.
   */
  public void run() {
    args.getFiles()
      .parallelStream()
      .map(this::parse)
      .map(x -> x.leftMap(this::semantics))
      .map(x -> x.leftMap(this::generate))
      .forEach(x -> x.fold(y -> y, this::logError));;
  }
  
  public static void main( String[] args )
  {
    Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.INFO);
    
    Compiler c = new Compiler(args);
    c.run();
  }
}