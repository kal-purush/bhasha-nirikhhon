package main;

//ExecutionUnit.java
public class ExecutionUnit {
 public void execute(ReservationStation reservationStation) {
     // Execute the instruction in the reservation station
     if (reservationStation != null && reservationStation.isReady()) {
         reservationStation.execute();
         System.out.println("Executed instruction: " + reservationStation.operation +
                 ", Dest: R" + reservationStation.getDestinationOperand() +
                 ", Cycle: " + reservationStation.getCycle());
     }
 }
}
