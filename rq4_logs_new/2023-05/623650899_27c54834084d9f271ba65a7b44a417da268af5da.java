package StepConnections;

public class InputConnections {
    private String inputName;
    private String connectedOutputName;
    private String connectedStepName;

    public InputConnections(String inputName, String connectedOutputName, String connectedStepName){
        this.inputName=inputName;
        this.connectedOutputName=connectedOutputName;
        this.connectedStepName=connectedStepName;
    }

    public String getInputName() {
        return inputName;
    }

    public void setInputName(String inputName) {
        this.inputName = inputName;
    }

    public String getConnectedOutputName() {
        return connectedOutputName;
    }

    public void setConnectedOutputName(String connectedOutputName) {
        this.connectedOutputName = connectedOutputName;
    }

    public String getConnectedStepName() {
        return connectedStepName;
    }

    public void setConnectedStepName(String connectedStepName) {
        this.connectedStepName = connectedStepName;
    }

}