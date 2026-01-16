package be.upgrade.it.adventofcode2020.day8;

import java.util.*;

public class HandheldGameConsole {
    private final String[] instructions;
    private int instructionPointer = 0;
    private long accumulator = 0;


    public HandheldGameConsole(String[] instructions) {
        this.instructions = instructions;
    }

    public long getAccumulator() {
        return accumulator;
    }

    public boolean runDiagnostics(){
        List<Integer> instructionPointerLog = new ArrayList<>();
        this.instructionPointer = 0;
        this.accumulator = 0;

        while(!instructionPointerLog.contains(instructionPointer) && instructionPointer < instructions.length){
            instructionPointerLog.add(instructionPointer);
            processCurrentInstruction();
        }

        return instructionPointer >= instructions.length;
    }

    private void processCurrentInstruction() {
        String instruction = instructions[instructionPointer];
        String[] s = instruction.split(" ");
        String operation = s[0];
        int argument = Integer.parseInt(s[1]);

        switch (operation){
            case "jmp":
                instructionPointer+=argument;
                break;
            case "nop":
                instructionPointer++;
                break;
            case "acc":
                accumulator+=argument;
                instructionPointer++;
                break;
        }
    }
    
    
    public static HandheldGameConsole generateFixed(String[] instructions){
        Set<Integer> jmpInstructionPointers = new HashSet<>();
        Set<Integer> nopInstructionPointers = new HashSet<>();
        for (int i = 0; i < instructions.length; i++) {
            if(instructions[i].startsWith("jmp")){
                jmpInstructionPointers.add(i);
            }
            if(instructions[i].startsWith("nop")){
                nopInstructionPointers.add(i);
            }
        }


        for (Integer jmpInstructionPointer : jmpInstructionPointers) {
            String[] temp = instructions.clone();
            String instruction = temp[jmpInstructionPointer];
            String replace = instruction.replace("jmp", "nop");
            temp[jmpInstructionPointer] = replace;
            HandheldGameConsole handheldGameConsole = new HandheldGameConsole(temp);
            boolean programEnded = handheldGameConsole.runDiagnostics();
            if(programEnded){
                return handheldGameConsole;
            }

        }

        for (Integer nopInstructionPointer : nopInstructionPointers) {
            String[] temp = instructions.clone();
            String instruction = temp[nopInstructionPointer];
            String replace = instruction.replace("nop", "jmp");
            temp[nopInstructionPointer] = replace;
            HandheldGameConsole handheldGameConsole = new HandheldGameConsole(temp);
            boolean programEnded = handheldGameConsole.runDiagnostics();
            if(programEnded){
                return handheldGameConsole;
            }

        }

        return null;
    }
}