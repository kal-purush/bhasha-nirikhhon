
/**
 * Write a description of class Controller here.
 * 
 * @author (your name) 
 * @version (a version number or a date)
 */
public class Controller
{
    CsvWriterString output = new CsvWriterString();

    /**
     * Constructor for objects of class Controller
     */
    public Controller()
    {

    }

    /**
     * An example of a method - replace this comment with your own
     * 
     * @param  y   a sample parameter for a method
     * @return     the sum of x and y 
     */
    public void runAll()
    {
        market1Stall1Line();
        market1Stall2Line();
        market2Stall2Line();
    }
    
    public void market1Stall1Line()
    {
        Market OneStallOneLineTrial1 = new Market(1.0);
        output.writeCsvFile(OneStallOneLineTrial1.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 1.1);
        Market OneStallOneLineTrial2 = new Market(1.0);
        output.writeCsvFile(OneStallOneLineTrial2.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 1.2);
        Market OneStallOneLineTrial3 = new Market(1.0);
        output.writeCsvFile(OneStallOneLineTrial3.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 1.3);
        Market OneStallOneLineTrial4 = new Market(1.0);
        output.writeCsvFile(OneStallOneLineTrial4.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 1.4);
        Market OneStallOneLineTrial5 = new Market(1.0);
        output.writeCsvFile(OneStallOneLineTrial5.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 1.5);
    }
    
    public void market1Stall2Line()
    {
        Market OneStallTwoLineTrial1 = new Market(2.0);
        output.writeCsvFile(OneStallTwoLineTrial1.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 2.1);
        Market OneStallTwoLineTrial2 = new Market(2.0);
        output.writeCsvFile(OneStallTwoLineTrial2.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 2.2);
        Market OneStallTwoLineTrial3 = new Market(2.0);
        output.writeCsvFile(OneStallTwoLineTrial3.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 2.3);
        Market OneStallTwoLineTrial4 = new Market(2.0);
        output.writeCsvFile(OneStallTwoLineTrial4.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 2.4);
        Market OneStallTwoLineTrial5 = new Market(2.0);
        output.writeCsvFile(OneStallTwoLineTrial5.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 2.5);
    }
    
    public void market2Stall2Line()
    {
        Market TwoStallTwoLineTrial1 = new Market(3.0);
        output.writeCsvFile(TwoStallTwoLineTrial1.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 3.1);
        Market TwoStallTwoLineTrial2 = new Market(3.0);
        output.writeCsvFile(TwoStallTwoLineTrial2.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 3.2);
        Market TwoStallTwoLineTrial3 = new Market(3.0);
        output.writeCsvFile(TwoStallTwoLineTrial3.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 3.3);
        Market TwoStallTwoLineTrial4 = new Market(3.0);
        output.writeCsvFile(TwoStallTwoLineTrial4.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 3.4);
        Market TwoStallTwoLineTrial5 = new Market(3.0);
        output.writeCsvFile(TwoStallTwoLineTrial5.runSim(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 3.5);
    }
}