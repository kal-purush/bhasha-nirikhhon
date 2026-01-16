package main;

import java.util.List;

public class DisplayStation {

    private static DisplayStation instance;
    private String description = "";

    private DisplayStation() {
        // Initialization logic if needed
    }
    public void printDescription() {
        System.out.println("Description:");
        System.out.println(description);
        System.out.println();
    }
    public void addDescription(String newDescription) {
        this.description += newDescription + "\n";
    }

    public static DisplayStation getInstance() {
        if (instance == null) {
            synchronized (DisplayStation.class) {
                if (instance == null) {
                    instance = new DisplayStation();
                }
            }
        }
        return instance;
    }

    public void printExecutingInstruction(Instruction instruction) {
        System.out.println("Executing Instruction:");
        System.out.println("+-----------+------------+------------+------------+");
        System.out.println("| Operation | Dest Reg   | Source Reg | Source Reg |");
        System.out.println("+-----------+------------+------------+------------+");
        printInstructionDetails(instruction);
    }

    public void printIssuingInstruction(Instruction instruction) {
        System.out.println("Issuing Instruction:");
        System.out.println("+-----------+------------+------------+------------+");
        System.out.println("| Operation | Dest Reg   | Source Reg | Source Reg |");
        System.out.println("+-----------+------------+------------+------------+");
        printInstructionDetails(instruction);
    }

    public void printWritingBackInstruction(Instruction instruction) {
        System.out.println("Writing Back Instruction:");
        System.out.println("+-----------+------------+------------+------------+");
        System.out.println("| Operation | Dest Reg   | Source Reg | Source Reg |");
        System.out.println("+-----------+------------+------------+------------+");
        printInstructionDetails(instruction);
    }

    public void printInstructionsInExecution(List<Instruction> instructions) {
        System.out.println("Instructions in Execution:");
        System.out.println("+-----------+------------+------------+------------+");
        System.out.println("| Operation | Dest Reg   | Source Reg | Source Reg |");
        System.out.println("+-----------+------------+------------+------------+");
        for (Instruction instruction : instructions) {
            printInstructionDetails(instruction);
        }
    }

    public void printInstructionsOnWait(List<Instruction> instructions) {
        System.out.println("Instructions on Wait:");
        System.out.println("+-----------+------------+------------+------------+");
        System.out.println("| Operation | Dest Reg   | Source Reg | Source Reg |");
        System.out.println("+-----------+------------+------------+------------+");
        for (Instruction instruction : instructions) {
            printInstructionDetails(instruction);
        }
    }

    public void printInstructionQueue(List<Instruction> instructions) {
        System.out.println("Instruction Queue:");
        System.out.println("+-----------+------------+------------+------------+");
        System.out.println("| Operation | Dest Reg   | Source Reg | Source Reg |");
        System.out.println("+-----------+------------+------------+------------+");
        for (Instruction instruction : instructions) {
            printInstructionDetails(instruction);
        }
    }
    public void printExecutingReservationStation(ReservationStation reservationStation) {
        System.out.println("Executing Reservation Station:");
        System.out.println("+-----------+------------+----------------+----------------+--------+--------+-------------------+");
        System.out.println("|   Busy    | Operation  | Source 1 Value | Source 2 Value |   Q1   |   Q2   | Effective Address |");
        System.out.println("+-----------+------------+----------------+----------------+--------+--------+-------------------+");
        System.out.printf("| %-9s | %-10s | %-14s | %-14s | %-6s | %-6s | %-17s |\n",
                reservationStation.isBusy(), reservationStation.operation,
                reservationStation.getSourceOperands()[0], reservationStation.getSourceOperands()[1],
                reservationStation.qSourceOperands[0], reservationStation.qSourceOperands[1],
                reservationStation.A);
        System.out.println("+-----------+------------+----------------+----------------+--------+--------+-------------------+");
    }

    public void printReservationStations(List<ReservationStation> reservationStations) {
        System.out.println("Reservation Stations:");
        System.out.println("+-----------+------------+----------------+----------------+--------+--------+-------------------+");
        System.out.println("|   Busy    | Operation  | Source 1 Value | Source 2 Value |   Q1   |   Q2   | Effective Address |");
        System.out.println("+-----------+------------+----------------+----------------+--------+--------+-------------------+");

        for (ReservationStation station : reservationStations) {
            System.out.printf("| %-9s | %-10s | %-14s | %-14s | %-6s | %-6s | %-17s |\n",
                    station.isBusy(), station.operation, station.getSourceOperands()[0],
                    station.getSourceOperands()[1], station.qSourceOperands[0], station.qSourceOperands[1],
                    station.A);
        }

        System.out.println("+-----------+------------+----------------+----------------+--------+--------+-------------------+");
    }
    public void printRegisterFile(RegisterFile registerFile) {
        System.out.println("Register File:");
        System.out.println("+------------+--------+------+");
        System.out.println("| Register   |  Hold  | Value|");
        System.out.println("+------------+--------+------+");

        for (int i = 0; i < registerFile.getRegisters().size(); i++) {
            System.out.printf("|    R%-3d     | %-6s | %-4d |\n", i, registerFile.getRegister(i).hold,
                    registerFile.getRegister(i).value);
        }

        System.out.println("+------------+--------+------+");
    }


    public void printMemory(Memory memory) {
        System.out.println("Memory:");
        System.out.println("+------------+--------+");
        System.out.println("| Address    | Value  |");
        System.out.println("+------------+--------+");

        for (int i = 0; i < memory.getSize(); i++) {
            System.out.printf("|    %-8d | %-6d |\n", i, memory.getData(i));
        }

        System.out.println("+------------+--------+");
    }

    private void printInstructionDetails(Instruction instruction) {
        System.out.printf("| %-9s | %-10s | %-10s | %-10s |\n",
                instruction.getOperation(), instruction.getDestinationRegister(),
                instruction.getSourceRegisters()[0], instruction.getSourceRegisters()[1]);
    }
}