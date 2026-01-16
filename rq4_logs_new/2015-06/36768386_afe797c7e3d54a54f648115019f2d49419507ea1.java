package com.dujay;

import com.dujay.codegen.CodeGenerator;

/**
 * Hello world!
 *
 */
public class Compiler 
{
  public void parse() throws Exception{
  }
  public void semantics() throws Exception {
    
  }
  public void generate() throws Exception {
    CodeGenerator generator = new CodeGenerator();
    generator.generate();
  }
  public void compile() {
    try {
      parse();
      semantics();
      generate();
    } catch(Exception e) {
      // just kill for now
      e.printStackTrace();
    }
  }
  public static void main( String[] args )
  {
    Compiler c = new Compiler();
    c.compile();
  }
}