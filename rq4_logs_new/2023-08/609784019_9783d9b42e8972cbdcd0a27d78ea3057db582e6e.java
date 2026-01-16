package application.viewControllers;

public class CurrentFXMLInstance {

    private static CurrentFXMLInstance instance;

    private String currentFXML;

    private CurrentFXMLInstance(String currentFXML) {
        this.currentFXML = currentFXML;
    }

    public static void initInstance(String currentFXML) {
        if(instance == null) {
            instance = new CurrentFXMLInstance(currentFXML);
        }
    }

    public static CurrentFXMLInstance getInstance() {
        return instance;
    }

    public String getCurrentFXML() {
        return currentFXML;
    }

    public void setCurrentFXML(String currentFXML) {
        this.currentFXML = currentFXML;
    }

    public void cleanCurrentFXMLInstance() {
        instance = null;
    }
}