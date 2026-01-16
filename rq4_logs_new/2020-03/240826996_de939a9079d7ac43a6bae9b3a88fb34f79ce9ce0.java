import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;
import javafx.scene.Node;
import javafx.stage.FileChooser;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

class ResearcherPane extends StackPane{
    private StackPane pane;
    private File entry;
    private Label fileDir;
    public ResearcherPane(Stage ps){
        pane = new StackPane();

        Label researcher_l = new Label("Researcher");
        researcher_l.setTranslateY(-300);

        createSubmission(ps);

        addChild(researcher_l);
    }

    private void addChild(Node child){
        pane.getChildren().addAll(child);

    };

    public void createSubmission(Stage ps){
        Button findBtn = new Button("open file");
        findBtn.setTranslateY(-100);
        findBtn.setTranslateX(200);
        // TODO: set this to have logic based on the account given as input
        findBtn.setOnAction(e -> {
            entry = selectFile(ps);
        });
        Button submitBtn = new Button("submit");

        submitBtn.setOnAction(e -> {
            System.out.println("saving. . .");
            try{
                saveFile(entry);
            }catch (IOException error){
                error.printStackTrace();
            }
        });

        fileDir = new Label("select a pdf file");


        fileDir.setTranslateY(-100);

        addChild(fileDir);
        addChild(findBtn);
        addChild(submitBtn);

    }

    //    private void saveFile(File file){
//        File dest = new File("\\All_Entries");
//        try {
//            Path src = Paths.get(file);
//            Path fnl = Paths.get(dest);
//            copy(src.toFile(),dest.toFile());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }
    private void saveFile(File source) throws IOException {
        File folder = new File("All Journals");
        folder.mkdirs();

        File dest = new File("All Journals\\NAME_"+source.getName());
        //boolean b = dest.mkdirs();

        InputStream is = null;
        OutputStream os = null;


        try {
            is = new FileInputStream(source);
            os = new FileOutputStream(dest);
            byte[] buffer = new byte[1024];
            int length;

            while ((length = is.read(buffer)) > 0) {
                os.write(buffer, 0, length);
            }
        } finally {
            is.close();
            os.close();
        }
    }

    private File selectFile(Stage ps){
        FileChooser fc = new FileChooser();
        fc.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("Text Files", "*.pdf")
        );

        File f = fc.showOpenDialog(ps);
        if(f != null) {
            fileDir.setText(f.getName());

            return f;
        }else{
            System.out.println("file not selected");
            return null;
        }
    }

    public StackPane getPane(){
        return pane;
    }


}