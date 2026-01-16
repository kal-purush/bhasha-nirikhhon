import java.io.FileInputStream; 
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.layout.FlowPane;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;  
import javafx.scene.image.WritableImage;
import javafx.scene.image.PixelWriter;
import javafx.scene.image.PixelReader;
import javafx.scene.paint.Color;
import javafx.stage.Stage;
import javafx.scene.chart.*;

public class Photoshop extends Application {

	int cumulativeGrey[] = new int[256];
	
	final NumberAxis xAxis = new NumberAxis(0, 255, 16);
    final NumberAxis yAxis = new NumberAxis(0, 52000, 6500);
      
    final LineChart<Number,Number> lineChart = 
            new LineChart<Number,Number>(xAxis,yAxis);
	
    @Override
    public void start(Stage stage) throws FileNotFoundException {
		stage.setTitle("Photoshop");

		//Read the image
		Image image = new Image(new FileInputStream("raytrace.jpg"));  

		//Create the graphical view of the image
		ImageView imageView = new ImageView(image); 
		
		//Invert object setup
		Button invert_button = new Button("Invert");
		
		//Gamma correction object setup
		Button gamma_button = new Button("Gamma Correct");
		TextField gamma_value = new TextField("");
		gamma_value.setPromptText("gamme value");
		gamma_value.setPrefSize(100, 24);
		
		//Contrast stretching object setup
		TextField r1 = new TextField();
		TextField s1 = new TextField();
		TextField r2 = new TextField();
		TextField s2 = new TextField();
		r1.setPromptText("r1");
		s1.setPromptText("s1");
		r2.setPromptText("r2");
		s2.setPromptText("s2");
		r1.setPrefSize(50, 24);
		s1.setPrefSize(50, 24);
		r2.setPrefSize(50, 24);
		s2.setPrefSize(50, 24);
		Button contrast_button = new Button("Contrast Stretching");
				
		//Histogram object setup
		Button grey_button = new Button("Grey");
		Button histogram_button = new Button("Histograms");
		
        xAxis.setLabel("Intensity");
        yAxis.setLabel("Number of pixel");
        
        lineChart.setTitle("RGB and Grey histogram graph for this image");
        
        lineChart.setMinSize(633, 555);
       
        //Cross correlation object setup
		Button cc_button = new Button("Cross Correlation");
		
		//Add all the event handlers (this is a minimal GUI - you may try to do better)
		invert_button.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                System.out.println("Invert");
				//At this point, "image" will be the original image
				//imageView is the graphical representation of an image
				//imageView.getImage() is the currently displayed image
				
				//Let's invert the currently displayed image by calling the invert function later in the code
				Image inverted_image=ImageInverter(imageView.getImage());
				//Update the GUI so the new image is displayed
				imageView.setImage(inverted_image);
            }
        });

		gamma_button.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                System.out.println("Gamma Correction");
                Image gamma_corrected_image=GammaCorrection(imageView.getImage(),gamma_value);
                imageView.setImage(gamma_corrected_image);
            }
        });

		contrast_button.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                System.out.println("Contrast Stretching");
                Image contrast_stretched_image=ContrastStretching(imageView.getImage(),r1,s1,r2,s2);
                imageView.setImage(contrast_stretched_image);
            }
        });
		
		grey_button.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                System.out.println("Grey");
                Image grey_image=Grey(imageView.getImage());
                imageView.setImage(grey_image);
            }
        });
		
		histogram_button.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                System.out.println("Histogram");
                Image histogram_equalized_image=HistogramEqualisation(imageView.getImage());
                imageView.setImage(histogram_equalized_image);
            }
        });
		
		cc_button.setOnAction(new EventHandler<ActionEvent>() {
			@Override
			public void handle(ActionEvent event) {
				System.out.println("Cross Correlation");
				Image cc_image=CrossCorrelation(imageView.getImage());
				imageView.setImage(cc_image);
			}
		});
		
		//Using a flow pane
		FlowPane root = new FlowPane();
		//Gaps between buttons
		root.setVgap(2);
        root.setHgap(5);

		//Add all the buttons and the image for the GUI
		root.getChildren().addAll(invert_button, gamma_value, gamma_button, r1, s1, r2, s2, contrast_button, grey_button, histogram_button, cc_button, imageView, lineChart);

		//Display to user
        Scene scene = new Scene(root, 1278, 579);
        stage.setScene(scene);
        stage.show();
    }

	//Example function of invert
	public Image ImageInverter(Image image) {
		//Find the width and height of the image to be process
		int width = (int)image.getWidth();
        int height = (int)image.getHeight();
		//Create a new image of that width and height
		WritableImage inverted_image = new WritableImage(width, height);
		//Get an interface to write to that image memory
		PixelWriter inverted_image_writer = inverted_image.getPixelWriter();
		//Get an interface to read from the original image passed as the parameter to the function
		PixelReader image_reader=image.getPixelReader();
		
		//Iterate over all pixels
		for(int y = 0; y < height; y++) {
			for(int x = 0; x < width; x++) {
				//For each pixel, get the colour
				Color color = image_reader.getColor(x, y);
				//Do something (in this case invert) - the getColor function returns colours as 0..1 doubles (we could multiply by 255 if we want 0-255 colours)
				color=Color.color(1.0-color.getRed(), 1.0-color.getGreen(), 1.0-color.getBlue());
				//Note: for gamma correction you may not need the divide by 255 since getColor already returns 0-1, nor may you need multiply by 255 since the Color.color function consumes 0-1 doubles.
				
				//Apply the new colour
				inverted_image_writer.setColor(x, y, color);
			}
		}
		return inverted_image;
	}
		
	
	
	//Function of gamma correction
	public Image GammaCorrection(Image image, TextField gamma_value) {
		
		int width = (int)image.getWidth();
	    int height = (int)image.getHeight();
		
		WritableImage gamma_corrected_image = new WritableImage(width, height);
		
		PixelWriter gamma_corrected_image_writer = gamma_corrected_image.getPixelWriter();
		PixelReader gamma_corrected_image_reader=image.getPixelReader();
			
		//Look-up table for gamma correction
		float gamma [] = new float[256];
		
		for(int a = 0; a < 256; a++) {
			
			gamma[a] = (float)(Math.pow((float)a/255.0,(1.0/Float.parseFloat(gamma_value.getText()))));
		}
		
		for(int y = 0; y < height; y++) {
			for(int x = 0; x < width; x++) {
				
				Color color = gamma_corrected_image_reader.getColor(x, y);
				
				color=Color.color(gamma[(int) (color.getRed()*255)], gamma[(int) (color.getGreen()*255)], gamma[(int) (color.getBlue()*255)]);
				
				gamma_corrected_image_writer.setColor(x, y, color);
			}
		}
		return gamma_corrected_image;
	}
	
	
	//Function of contrast stretching
	public Image ContrastStretching(Image image, TextField r1, TextField s1, TextField r2, TextField s2) {
		
		int width = (int)image.getWidth();
        int height = (int)image.getHeight();
		
		WritableImage contrast_stretched_image = new WritableImage(width, height);
		
		PixelWriter contrast_stretched_image_writer = contrast_stretched_image.getPixelWriter();
		PixelReader image_reader=image.getPixelReader();
		
		//2 control points
		double x1 = Double.parseDouble(r1.getText());
		double y1 = Double.parseDouble(s1.getText());
		double x2 = Double.parseDouble(r2.getText());
		double y2 = Double.parseDouble(s2.getText());
		
		for(int y = 0; y < height; y++) {
			for(int x = 0; x < width; x++) {
				
				Color color = image_reader.getColor(x, y);
				
				color=Color.color(calculateContrast(color.getRed(), x1, y1, x2, y2), calculateContrast(color.getGreen(), x1, y1, x2, y2), calculateContrast(color.getBlue(), x1, y1, x2, y2));
			
				contrast_stretched_image_writer.setColor(x, y, color);
			}
		}
		return contrast_stretched_image;
	}
	
	public double calculateContrast(double pixel, double x1, double y1, double x2, double y2) {
		double c = 0;
		if (pixel*255.0 < x1) {
			c = pixel * 255.0 * y1 / x1;
		} else if (pixel*255.0 >= x2) {
			c = ((pixel * 255.0 - x2) * (255.0 - y2)/(255.0 - x2)) + y2;
		} else {
			c = ((pixel * 255.0 - x1) * (y2 - y1)/(x2 - x1)) + y1;
		}
		
		return c/255.0;
	}
	
	//Setup histogram for 4 color channel
		int redData[] = new int[256];
		int greenData[] = new int[256];
		int blueData[] = new int[256];
		int greyData[] = new int[256];
		
	//Function of grey
	public Image Grey(Image image) {
		
		int width = (int)image.getWidth();
        int height = (int)image.getHeight();
		
		WritableImage grey_image = new WritableImage(width, height);
		
		PixelWriter grey_image_writer = grey_image.getPixelWriter();
		PixelReader grey_image_reader=image.getPixelReader();
		
		for(int y = 0; y < height; y++) {
			for(int x = 0; x < width; x++) {
				
				Color color = grey_image_reader.getColor(x, y);
				
				double r = color.getRed()*255;
				double g = color.getGreen()*255;
				double b = color.getBlue()*255;
				double gr = (r+g+b)/3;
												
				redData[(int) (color.getRed()*255)]++; 
				greenData[(int)(color.getGreen()*255)]++; 
				blueData[(int)(color.getBlue()*255)]++;
				greyData[(int)(((color.getRed()*255)+(color.getGreen()*255)+(color.getBlue()*255))/3)]++;
				
				color = Color.color(gr/255, gr/255, gr/255);
				
				grey_image_writer.setColor(x, y, color);
			}
		}
		
		for(int a=0; a<256; a++) {
			System.out.println("Red: " + redData[a] + " Green: " + greenData[a] + " Blue: " + blueData[a] + " Grey: " + greyData[a]);
		}
		
		cumulativeGrey[0] = greyData[0];
		
		for(int a=1; a<256; a++) {
			
			cumulativeGrey[a] = cumulativeGrey[a-1] + greyData[a];
			System.out.println(" Grey: " + cumulativeGrey[a]);
		}
		
		return grey_image;
	}
	
	//Function of histogram equalisation
	public Image HistogramEqualisation(Image image) {
		
		int width = (int)image.getWidth();
        int height = (int)image.getHeight();
		double mapping[] = new double[256];
        
		WritableImage histogram_equalized_image = new WritableImage(width, height);
		
		PixelWriter histogram_equalized_image_writer = histogram_equalized_image.getPixelWriter();
		PixelReader histogram_equalized_image_reader=image.getPixelReader();
		
		XYChart.Series red = new XYChart.Series<Number,Number>();
		XYChart.Series grey = new XYChart.Series<Number,Number>();
        XYChart.Series green = new XYChart.Series<Number,Number>();
        XYChart.Series blue = new XYChart.Series<Number,Number>();
        
        red.setName("Red");
        grey.setName("Grey");
        green.setName("Green");
        blue.setName("Blue");
        
		
		for(int a = 0; a < 256; a++) {
			red.getData().add(new XYChart.Data(a, redData[a]));
			grey.getData().add(new XYChart.Data(a, greyData[a]));
			green.getData().add(new XYChart.Data(a, greenData[a]));
			blue.getData().add(new XYChart.Data(a, blueData[a]));
			
		}
		
		lineChart.getData().addAll(red, grey, green, blue);		
		
		for(int a = 0; a < 256; a++) {
			mapping[a]=255.0*((double)cumulativeGrey[a]/(width*height));
		}
		
		for(int y = 0; y < height; y++) {
			for(int x = 0; x < width; x++) {
				
				Color color = histogram_equalized_image_reader.getColor(x, y);
				int oi = (int) (color.getRed()* 255.0);
				double new_intensity = mapping[oi]/255.0;
				color=Color.color(new_intensity, new_intensity, new_intensity);
				histogram_equalized_image_writer.setColor(x, y, color);
			}
		}
		return histogram_equalized_image;
	}
	
	//Function of cross correlation
	public Image CrossCorrelation(Image image) {
		int width = (int)image.getWidth();
        int height = (int)image.getHeight();
        
        WritableImage cc_image = new WritableImage(width, height);
		
		PixelWriter cc_image_writer = cc_image.getPixelWriter();
		PixelReader cr=image.getPixelReader();
		
		ArrayList<Integer> sumRed = new ArrayList<Integer>();
		ArrayList<Integer> sumGreen = new ArrayList<Integer>();
		ArrayList<Integer> sumBlue = new ArrayList<Integer>();
		
		for(int y = 2; y < height-2; y++) {
			for(int x = 2; x < width-2; x++) {
				
				sumRed.add((int)
				(cr.getColor(x-2, y-2).getRed()*(-4)*255 +
				 cr.getColor(x-1, y-2).getRed()*(-1)*255 +
				 cr.getColor(x  , y-2).getRed()*( 0)*255 +
				 cr.getColor(x+1, y-2).getRed()*(-1)*255 +
				 cr.getColor(x+2, y-2).getRed()*(-4)*255 + 
				 
				 cr.getColor(x-2, y-1).getRed()*(-1)*255 +
				 cr.getColor(x-1, y-1).getRed()*( 2)*255 +
				 cr.getColor(x  , y-1).getRed()*( 3)*255 +
				 cr.getColor(x+1, y-1).getRed()*( 2)*255 +
				 cr.getColor(x+2, y-1).getRed()*(-1)*255 +
				 
				 cr.getColor(x-2, y  ).getRed()*( 0)*255 +
				 cr.getColor(x-1, y  ).getRed()*( 3)*255 +
				 cr.getColor(x  , y  ).getRed()*( 4)*255 +
				 cr.getColor(x+1, y  ).getRed()*( 3)*255 +
				 cr.getColor(x+2, y  ).getRed()*( 0)*255 +
				 
				 cr.getColor(x-2, y+1).getRed()*(-1)*255 +
				 cr.getColor(x-1, y+1).getRed()*( 2)*255 +
				 cr.getColor(x  , y+1).getRed()*( 3)*255 +
				 cr.getColor(x+1, y+1).getRed()*( 2)*255 +
				 cr.getColor(x+2, y+1).getRed()*(-1)*255 +
				 
				 cr.getColor(x-2, y+2).getRed()*(-4)*255 +
				 cr.getColor(x-1, y+2).getRed()*(-1)*255 +
				 cr.getColor(x  , y+2).getRed()*( 0)*255 +
				 cr.getColor(x+1, y+2).getRed()*(-1)*255 +
				 cr.getColor(x+2, y+2).getRed()*(-4)*255));
				
				sumGreen.add((int)
				(cr.getColor(x-2, y-2).getGreen()*(-4)*255 +
				 cr.getColor(x-1, y-2).getGreen()*(-1)*255 +
				 cr.getColor(x  , y-2).getGreen()*( 0)*255 +
				 cr.getColor(x+1, y-2).getGreen()*(-1)*255 +
				 cr.getColor(x+2, y-2).getGreen()*(-4)*255 +
				 
				 cr.getColor(x-2, y-1).getGreen()*(-1)*255 +
				 cr.getColor(x-1, y-1).getGreen()*( 2)*255 +
				 cr.getColor(x  , y-1).getGreen()*( 3)*255 +
				 cr.getColor(x+1, y-1).getGreen()*( 2)*255 +
				 cr.getColor(x+2, y-1).getGreen()*(-1)*255 +
				 
				 cr.getColor(x-2, y  ).getGreen()*( 0)*255 +
				 cr.getColor(x-1, y  ).getGreen()*( 3)*255 +
				 cr.getColor(x  , y  ).getGreen()*( 4)*255 +
				 cr.getColor(x+1, y  ).getGreen()*( 3)*255 +
				 cr.getColor(x+2, y  ).getGreen()*( 0)*255 +
				 
				 cr.getColor(x-2, y+1).getGreen()*(-1)*255 +
				 cr.getColor(x-1, y+1).getGreen()*( 2)*255 +
				 cr.getColor(x  , y+1).getGreen()*( 3)*255 +
				 cr.getColor(x+1, y+1).getGreen()*( 2)*255 +
				 cr.getColor(x+2, y+1).getGreen()*(-1)*255 +
				 
				 cr.getColor(x-2, y+2).getGreen()*(-4)*255 +
				 cr.getColor(x-1, y+2).getGreen()*(-1)*255 +
				 cr.getColor(x  , y+2).getGreen()*( 0)*255 +
				 cr.getColor(x+1, y+2).getGreen()*(-1)*255 +
				 cr.getColor(x+2, y+2).getGreen()*(-4)*255));
				
				sumBlue.add((int) 
				(cr.getColor(x-2, y-2).getBlue()*(-4)*255 +
				 cr.getColor(x-1, y-2).getBlue()*(-1)*255 +
				 cr.getColor(x  , y-2).getBlue()*( 0)*255 +
				 cr.getColor(x+1, y-2).getBlue()*(-1)*255 +
				 cr.getColor(x+2, y-2).getBlue()*(-4)*255 +
				 
				 cr.getColor(x-2, y-1).getBlue()*(-1)*255 +
				 cr.getColor(x-1, y-1).getBlue()*( 2)*255 +
				 cr.getColor(x  , y-1).getBlue()*( 3)*255 +
				 cr.getColor(x+1, y-1).getBlue()*( 2)*255 +
				 cr.getColor(x+2, y-1).getBlue()*(-1)*255 +
				 
				 cr.getColor(x-2, y  ).getBlue()*( 0)*255 +
				 cr.getColor(x-1, y  ).getBlue()*( 3)*255 +
				 cr.getColor(x  , y  ).getBlue()*( 4)*255 +
				 cr.getColor(x+1, y  ).getBlue()*( 3)*255 +
				 cr.getColor(x+2, y  ).getBlue()*( 0)*255 +
				 
				 cr.getColor(x-2, y+1).getBlue()*(-1)*255 +
				 cr.getColor(x-1, y+1).getBlue()*( 2)*255 +
				 cr.getColor(x  , y+1).getBlue()*( 3)*255 +
				 cr.getColor(x+1, y+1).getBlue()*( 2)*255 +
				 cr.getColor(x+2, y+1).getBlue()*(-1)*255 +
				 
				 cr.getColor(x-2, y+2).getBlue()*(-4)*255 +
				 cr.getColor(x-1, y+2).getBlue()*(-1)*255 +
				 cr.getColor(x  , y+2).getBlue()*( 0)*255 +
				 cr.getColor(x+1, y+2).getBlue()*(-1)*255 +
				 cr.getColor(x+2, y+2).getBlue()*(-4)*255));
								
			}
		}
		
		Object norRed[] = sumRed.toArray();
		Object norGreen[] = sumGreen.toArray();
		Object norBlue[] = sumBlue.toArray();
		
		Collections.sort(sumRed);
		Collections.sort(sumGreen);
		Collections.sort(sumBlue);
		
		int minRed = sumRed.get(0);
		int minGreen = sumGreen.get(0);
		int minBlue = sumBlue.get(0);
		
		int maxRed = sumRed.get(sumRed.size()-1);
		int maxGreen = sumGreen.get(sumGreen.size()-1);
		int maxBlue = sumBlue.get(sumBlue.size()-1);
		
		int min = Math.min(minBlue, Math.min(minRed, minGreen));
		int max = Math.max(maxBlue, Math.max(maxRed, maxGreen));
		
		for(int i=0; i<norRed.length; i++) {
			norRed[i] = (((int)norRed[i]-min)*255/(max-min));
		}
		
		for(int i=0; i<norGreen.length; i++) {
			norGreen[i] = ((int)norGreen[i]-min)*255/(max-min);
		}
		
		for(int i=0; i<norBlue.length; i++) {
			norBlue[i] = ((int)norBlue[i]-min)*255/(max-min);
		}
		
		int counter = 0;
		
		for(int y = 0; y < height-4; y++) {
			for(int x = 0; x < width-4; x++) {
				
				Color color = cr.getColor(x, y);
		
				color=Color.color(((int)norRed[x+counter])/255.0, ((int)norGreen[x+counter])/255.0, ((int)norBlue[x+counter])/255.0);
				cc_image_writer.setColor(x, y, color);
			}
			counter = counter + width - 4 ;
		}
		return cc_image;
	}
	
    public static void main(String[] args) {
        launch();
    }

}