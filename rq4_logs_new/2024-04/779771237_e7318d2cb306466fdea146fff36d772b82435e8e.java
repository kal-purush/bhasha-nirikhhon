package compiler;

import java.util.HashMap;
import org.objectweb.asm.*;

/**
 * This class overrides the methods in the antlr-generated BaseVisitor class to traverse a parse tree
 * 
 * @author Kirin Sharma
 * @version 1.0
 * Assignment 5
 * CS322 - Compiler Construction
 * Spring 2024
 */


public class CustomVisitor<T> extends lexparse.KnightCodeBaseVisitor<T>
{
    
    @SuppressWarnings("rawtypes")
    public HashMap<String, Variable> symbolTable; // the symbol table for all variables
    public int indexCount; // keeps track of current memory location for variable assignment
    public String outputName; // the name of the output file
    public ClassWriter cw; // the ASM class writer used in various methods to write bytecode
    public MethodVisitor mv; // the ASM method visitor used in various methods to write bytecode

    /**
     * Preferred constructor initializes filename
     */
    public CustomVisitor(String fileName)
    {
        symbolTable = new HashMap<String, Variable>(); 
        indexCount = 0;
        outputName = fileName;
    } // end preferred constructor


    @Override
    public T visitFile(lexparse.KnightCodeParser.FileContext ctx) 
    {
        // System.out.println("Write header of .class file");
        // Create class and main method for header of class file
        cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        cw.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC, outputName, null, "java/lang/Object",null);
        mv = cw.visitMethod(Opcodes.ACC_PUBLIC+Opcodes.ACC_STATIC, "main", "([Ljava/lang/String;)V", null, null);
        mv.visitCode();
       
        return visitChildren(ctx); 
    } // end visitFile


    @Override
    public T visitVariable(lexparse.KnightCodeParser.VariableContext ctx) 
    {
        String dataType = ctx.getChild(0).getText(); // gets the left grandchild of the variable subtree, which is the data type
        String identifier = ctx.getChild(1).getText(); // gets the right grandchild of the variable subtree, which is the name of the identifier
        
        if(dataType.equals("INTEGER"))
        {
            symbolTable.put(identifier, new Variable<Integer>("Integer")); // declares a new Integer in the symbol table with a null value
        }
        else if(dataType.equals("STRING"))
        {
            symbolTable.put(identifier, new Variable<String>("String")); // declares a new String in the symbol table with a null value
        }

        // System.out.println("Created a variable.");
        
        return visitChildren(ctx); 
    } // end visitVariable


    @Override
    public T visitSetvar(lexparse.KnightCodeParser.SetvarContext ctx)
    {
        String identifier = ctx.getChild(1).getText();

        if(symbolTable.get(identifier).getDataType().equals("Integer"))
        {
            // Update symbol table
           Integer value = Integer.parseInt(ctx.getChild(3).getText());
           symbolTable.get(identifier).setData(value);
           symbolTable.get(identifier).setMemoryLocation(indexCount);
           indexCount++; 

           // Load integer into memory
           mv.visitLdcInsn((int) symbolTable.get(identifier).getData());
           mv.visitVarInsn(Opcodes.ISTORE, symbolTable.get(identifier).getMemoryLocation());
        }
        else
        {
            String value = ctx.getChild(3).getText();
            symbolTable.get(identifier).setData(value);
            symbolTable.get(identifier).setMemoryLocation(indexCount);
            indexCount++; 

           // Load String into memory
           mv.visitLdcInsn(symbolTable.get(identifier).getData());
           mv.visitVarInsn(Opcodes.ASTORE, symbolTable.get(identifier).getMemoryLocation());
        }

        return visitChildren(ctx);
    } // end visitSetVar


    @Override
    public T visitPrint(lexparse.KnightCodeParser.PrintContext ctx)
    {
        String identifier = ctx.getChild(1).getText();

        if(symbolTable.get(identifier).getDataType().equals("Integer"))
        {
            // Print an integer
            mv.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
            mv.visitVarInsn(Opcodes.ILOAD, symbolTable.get(identifier).getMemoryLocation());
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/PrintStream", "println", "(I)V", false);
        }
        else
        {
            // Print a String
            mv.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
            mv.visitVarInsn(Opcodes.ALOAD, symbolTable.get(identifier).getMemoryLocation());
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V", false);
        }
        

        // System.out.println("Printed the value.");

        return visitChildren(ctx);
    } // end visitPrint

    
    public void writeFooter()
    {
        //System.out.println("Wrote the footer of the .class file.");

        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0,0);
        mv.visitEnd();

    } // end writeFooter

    /**
     * Returns the .class file as an array of bytes
     * @return
     */
    public byte[] getByteArray()
    {
        return cw.toByteArray();
    } // end getByteArray

} // end class