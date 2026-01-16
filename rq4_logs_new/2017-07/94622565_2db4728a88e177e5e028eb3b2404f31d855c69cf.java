/**
 * Provides input sequences.
 */
class InputSequenceProvider {
  public String[] firstSequence;
   
  public InputSequenceProvider(String[] args) {
    firstSequence = args;
  } 
  
  /**
   * Gets the next input sequence.
   * When called for the first time, returns the value of firstSequence field as a sequence,
   * and sets that field empty.
   * When the subsequent callings occur, requests sequences from the console. 
   * 
   * @return the sequence.
   */ 
  public String[] provideInputSequence() {
    if (firstSequence.length == 0) {
      ConsoleReader reader = new ConsoleReader();  
      return reader.readSequence();
    } else {
      String[] tempSequence = firstSequence;
      firstSequence = new String[0];
      return tempSequence;
    }
  }
}