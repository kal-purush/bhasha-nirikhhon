import click

'''from ImgClass.src import DataClass
from ImgClass.src import model as m

from ImgClass.src import DataHandler as DH'''
import os
from ImgClass.src import training_command
from ImgClass.src import predict_command
from ImgClass.src import DataClass


data = DataClass.Parameters()

@click.group()
@click.version_option(package_name="image_classifier")
def main():
    """Image Classifier is a CLI tool that creates a machine learning model to classify images"""
    
    pass

@main.command()
@click.option( "--model", "-m", type=click.STRING, help="If a model exists then use the model that will be tested.")
@click.option("--training","-tr",type=click.STRING,required=True,help="Adds the data that the model will be trained on.")
@click.option("--epochs","-e",type=int,help = "Changes the number of epochs that will be done during training.")
@click.option("--batch","-b",type=int,help = "Changes the batch number that will be used during training.")
@click.option("--height","-h",type=int,help = "Changes the height of the images during training.")
@click.option("--confidence_threshold","-ct",type=int,help = "Changes the height of the images during training.")
@click.option("--width","-w",type=int,help = "Changes the width of the images during training.")
@click.option("--output","-o",type=click.STRING,required=True,help="Changes where the model and analysis will be saved.")
@click.option("--json", "-j", is_flag = False, help = "If this flag is present, a JSON file is generated")


def train(training, batch, epochs, model, height, width, confidence_threshold, output, json):
    """Trains on data to create a new model."""
    jsonV = False    
    epochsV = 8
    batchV = 32
    heightV = 400
    widthV = 400
    modelV = ""
    trainingV = ""
    testingV = ""
    ctV = -1
    outputV = "Output"
    saveV = ""

    if epochs:
        epochsV = epochs
        data.num_epochs = epochs

    if confidence_threshold:
        ctV = confidence_threshold
        data.num_confidence = confidence_threshold
        
    if batch:
        batchV = batch
        data.batch_size = batch
       
    if height:
        heightV = height
        data.height_pixels = height
        
    if width:
        widthV = width
        data.width_pixels = width
        
    if output:
        print("I am printing output", output)
        if not os.path.isdir(output):
            raise ValueError("Output is not a valid directory.")
        elif not os.path.exists(output):
            raise FileNotFoundError("This path does not exist.")
        outputV = output
        data.output_location = output
    if model:
        # if mode is not in correct directory throw an exception
        if not os.path.isdir(model):
            raise ValueError("Model is not in a directory.")
        elif not os.path.exists(model):
            raise FileNotFoundError("This path does not exist.")
        modelV = model
        print(model)
    if training:
        # if mode is not in correct directory throw an exception
        if not os.path.isdir(training):
            raise ValueError("Training data set is not in a directory.")
        elif not os.path.exists(training):
            raise FileNotFoundError("This path does not exist.")
        trainingV = training
        print(training)
    if json:
        jsonV = True
        data.json = True
    
    training_command.run(epochsV, batchV, trainingV, testingV, heightV, widthV, modelV, ctV, outputV, jsonV)
    
    return


@main.command()
@click.option( "--model", "-m", type=click.STRING, required=True, help="If a model exists then use the model that will be tested.")
@click.option("--testing","-te",type=click.STRING, required=True, help = "Adds the data that the model will be tested on.")
# @click.option("--epochs","-e",type=int,help = "Changes the number of epochs that will be done during training.")
# @click.option("--batch","-b",type=int,help = "Changes the batch number that will be used during training.")
# @click.option("--height","-h",type=int,help = "Changes the height of the images during training.")
@click.option("--confidence_threshold","-ct",type=int,help = "Changes the weight of the images during training.")
# @click.option("--width","-w",type=int,help = "Changes the width of the images during training.")
@click.option("--output","-o",type=click.STRING,required=True,help="Changes where the file will be created to store analysis about the model creation process.")
@click.option("--nr", is_flag = True, help="Decide whether the report is generated or not. If nothing is entered the report will be generated.")
@click.option("--json", "-j", is_flag = False, help = "If this flag is present, a JSON file is generated")

# def predict( testing, batch, epochs, model, height, width, confidence_threshold, output, nr):
def predict(output, testing, model, confidence_threshold, nr, json):
    """Runs Classification Prediction using provided model."""
    jsonV = False
    modelV = ""
    testingV = ""
    ctV = -1
    make_reportV = True
    outputV = ""

    print("Testing", testing)

    # if epochs:
    #     epochsV = epochs
    #     data.num_epochs = epochs
        
    if confidence_threshold:
        ctV = confidence_threshold
        data.num_confidence = confidence_threshold
        
    # if batch:
    #     batchV = batch
    #     data.batch_size = batch
        
    # if height:
    #     heightV = height
    #     data.height_pixels = height
        
    # if width:
    #     widthV = width
    #     data.width_pixels = width
        
    if output:
        if not os.path.isdir(output):
            raise ValueError("Output is not a valid directory.")
        elif not os.path.exists(output):
            raise FileNotFoundError("This path does not exist.")
        outputV = output
        data.output_location = output
        

    if model:
        # if mode is not in correct directory throw an exception
        if not os.path.isdir(model):
            raise ValueError("Model is not in a directory.")
        elif not os.path.exists(model):
            raise FileNotFoundError("This path does not exist.")
        modelV = model
        
    if testing:
        # if mode is not in correct directory throw an exception
        if not os.path.isdir(testing):
            raise ValueError("The testing data set is not in a directory.")
        elif not os.path.exists(testing):
            raise FileNotFoundError("This path does not exist.")
        testingV = testing
        
    if nr:
        make_reportV = False
        data.make_report = False
    
    if json:
        jsonV = True
        data.json = True


    
    # predict_command.run(epochsV, batchV, trainingV, testingV, heightV, widthV, modelV, ctV, outputV, make_reportV)
    predict_command.run(testingV, modelV, ctV, outputV, make_reportV, jsonV)
    print("test")
    return

if __name__ == "__main__":
    main()