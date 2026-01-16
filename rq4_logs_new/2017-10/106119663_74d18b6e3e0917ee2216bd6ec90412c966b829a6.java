/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package videoplayer;

import java.io.File;
import java.net.URL;
import java.util.ResourceBundle;
import javafx.beans.InvalidationListener;
import javafx.beans.Observable;
import javafx.beans.binding.Bindings;
import javafx.beans.property.DoubleProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Slider;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;
import javafx.scene.media.MediaView;
import javafx.stage.FileChooser;
import javafx.util.Duration;

/**
 *
 * @author Karan
 */
public class FXMLDocumentController implements Initializable {
    
    private String filePath;
    private MediaPlayer mediaPlayer;
    @FXML
    private MediaView mediaView;
    @FXML
    private Slider vSlider;
    
    @FXML
    private Slider seekSlider;
    @FXML
    private VBox vbox;
    @FXML
    private HBox hBox;
    
    @FXML
    private void handleButtonAction(ActionEvent event) {
        
        FileChooser fileChooser=new FileChooser();
        FileChooser.ExtensionFilter extensionFilter=new FileChooser.ExtensionFilter("Select a File (*.mp4)","*.mp4");
        fileChooser.getExtensionFilters().add(extensionFilter);
        File file=fileChooser.showOpenDialog(null);
        try{
            filePath =file.toURI().toString();    
            if(filePath!=null){
                Media media=new Media(filePath);
                mediaPlayer=new MediaPlayer(media);
                mediaView.setMediaPlayer(mediaPlayer);
                DoubleProperty width=mediaView.fitWidthProperty();
                DoubleProperty height=mediaView.fitHeightProperty();
                width.bind(Bindings.selectDouble(mediaView.sceneProperty(), "width"));
                height.bind(Bindings.selectDouble(mediaView.sceneProperty(), "height"));
                vSlider.setValue(mediaPlayer.getVolume()*100);
                vSlider.valueProperty().addListener(new InvalidationListener() {
                    @Override
                    public void invalidated(Observable observable) {
                        mediaPlayer.setVolume(vSlider.getValue()/100);
                    }
                });
                                
                seekSlider.maxProperty().bind(Bindings.createDoubleBinding(() -> mediaPlayer.getTotalDuration().toSeconds(),mediaPlayer.totalDurationProperty()));
                
                mediaPlayer.currentTimeProperty().addListener(new ChangeListener<Duration>() {
                    @Override
                    public void changed(ObservableValue<? extends Duration> observable, Duration oldValue, Duration newValue) {
                        seekSlider.setValue(newValue.toSeconds());
                        
                    }
                });
                
                seekSlider.setOnMouseClicked(new EventHandler<MouseEvent>() {
                    @Override
                    public void handle(MouseEvent event) {
                        mediaPlayer.seek(Duration.seconds(seekSlider.getValue()));
                    }
                });
                if(mediaPlayer.getMedia()!=null){
                mediaPlayer.stop();
                mediaPlayer.play();
                }
            }
        }catch(Exception e){
        }
    }
      
    @FXML
    private void playVideo(ActionEvent ae){
        mediaPlayer.play();
        mediaPlayer.setRate(1);
    }
    @FXML
    private void pauseVideo(ActionEvent ae){
        mediaPlayer.pause();
    }
     @FXML
    private void fastVideo(ActionEvent ae){
        mediaPlayer.setRate(1.5);
    }
    @FXML
    private void fasterVideo(ActionEvent ae){
        mediaPlayer.setRate(3.0);
        
    }
    @FXML
    private void slowVideo(ActionEvent ae){
        
            mediaPlayer.setRate(.75);
        
    }
    @FXML
    private void slowerVideo(ActionEvent ae){
        mediaPlayer.setRate(0.25);
    } 
    @FXML
    private void stopVideo(ActionEvent ae){
        mediaPlayer.stop();
    } 
    @FXML
    private void exitVideo(ActionEvent ae){
        System.exit(0);
    }
    @Override
    public void initialize(URL url, ResourceBundle rb) {
        // TODO
    }    
    
}