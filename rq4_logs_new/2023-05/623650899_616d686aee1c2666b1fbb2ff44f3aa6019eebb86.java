package Steps;

import DataTypes.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CommandLineStep extends Step{
    private StringType command;
    private StringType arguments;
    private StringType result;
    private static double stepAvgDuration=0;
    private static int stepStartUpCount=0;

    public CommandLineStep() {
        super("Command Line", false);
        this.command = new StringType(StepInputNameEnum.COMMAND.toString(),true);
        this.arguments=new StringType(StepInputNameEnum.OPERATION.toString(),true);
        this.result=new StringType(StepOutputNameEnum.RESULT.toString(),false);
        this.command.setMandatory(true);
        this.arguments.setMandatory(false);
    }

    public CommandLineStep(StringType command, StringType arguments) {
        this();
        this.command=command;
        this.arguments=arguments;
        this.command.setMandatory(true);
        this.arguments.setMandatory(false);
    }



    @Override
    protected void outerRunStepFlow(){
        try{
            this.runStepFlow();
        }
        catch (Exception e){
            this.setSummaryLine("Exception: " + e.getMessage());
            this.setStatus(Status.Success);
        }
    }

    @Override
    protected void runStepFlow() throws Exception {
        String commandLine=this.command.getData();
        String arguments=this.arguments.getData();
        addLog("About to invoke "+ commandLine + arguments);
        if(commandLine==null||commandLine.isEmpty())throw new Exception("empty command entered");
        String commandLineWithArguments="cmd.exe /c "+commandLine+" "+arguments;
        String[] strings=commandLineWithArguments.split(" ");

        ProcessBuilder processBuilder=new ProcessBuilder(strings);
        Process  process= processBuilder.start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        StringBuilder builder = new StringBuilder();
        String line = null;
        while ( (line = reader.readLine()) != null) {
            builder.append(line);
            builder.append(" ");
        }
        String resultString = builder.toString();
        result.setData(resultString);

        setSummaryLine("successfully invoked "+commandLine+arguments);
        setStatus(Status.Success);
    }

    @Override
    public void setInputs(DataType... inputs) {
        for(DataType input: inputs){
            if(input.getEffectiveName().equals(command.getEffectiveName())) {
                this.command.setData((String) input.getData());
                this.command.setMandatory(true);
            }
            if(input.getEffectiveName().equals(arguments.getEffectiveName())) {
                this.arguments.setData((String) input.getData());
                this.arguments.setMandatory(false);
            }
        }
    }

    @Override
    public void setInputByName(DataType input, String inputName) {
        if(inputName.equals(command.getEffectiveName()))
            this.command.setData((String) input.getData());
        if(inputName.equals(arguments.getEffectiveName()))
            this.arguments.setData((String) input.getData());
    }

    @Override
    public ArrayList<DataType> getOutputs(String... outputNames) {
        ArrayList<DataType> outputsArray=new ArrayList<>();
        for(String outputName: outputNames){
            if(this.result.getEffectiveName().equals(outputName))
                outputsArray.add(this.result);
        }
        return outputsArray;
    }

    @Override
    public ArrayList<DataType> getAllData() {
        ArrayList<DataType> allData=new ArrayList<>();
        allData.add(this.command);
        allData.add(this.arguments);
        allData.add(this.result);
        return  allData;
    }

    @Override
    protected void updateStaticTimers() {
        stepStartUpCount= startUpCounter;
        stepAvgDuration=durationAvgInMs;
    }

    public static int getStepStartUpCount() {
        return stepStartUpCount;
    }

    public static double getStepAvgDuration() {
        return stepAvgDuration;
    }

    public void clearDataMembers(){
        this.command.eraseData();
        this.arguments.eraseData();
        this.result.eraseData();
    }
}