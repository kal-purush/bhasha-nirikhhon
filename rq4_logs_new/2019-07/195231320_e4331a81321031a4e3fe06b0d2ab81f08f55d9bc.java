package Generator;

import Model.ConstCollection;
import Model.ControlTypes;

public class GeneratorThread extends Thread implements Runnable {

  private int minNumber = 0;
  private int maxNumber = 0;
  private ControlTypes type = null;
  private boolean contrast = false;
  private boolean disabledControls = false;
  private boolean isSorted = false;
  private int threadNumber;
  private boolean noise = false;

  public GeneratorThread(int min, int max, ControlTypes type, boolean contrast, boolean disabledControls, boolean noise, boolean isSorted, int threadNumber){
    this.minNumber = min;
    this.maxNumber = max;
    if (type != null) {
      this.type = type;
    }
    this.noise = noise;
    this.contrast = contrast;
    this.disabledControls = disabledControls;
    this.isSorted = isSorted;
    this.threadNumber = threadNumber + 1;
  }

  @Override
  public void run(){
      Generator gen = new Generator();
      gen.createSamples(type, minNumber, maxNumber, contrast, disabledControls, noise, isSorted, threadNumber);
  }
}