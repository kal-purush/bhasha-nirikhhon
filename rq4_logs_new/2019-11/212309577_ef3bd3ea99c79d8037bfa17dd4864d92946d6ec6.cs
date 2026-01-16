using System;
using System.Collections.Generic;
using Emgu.CV;
using Emgu.CV.Structure;

using System.IO;
using System.Xml;
using System.Windows.Forms;
using UniFCR_Controller;

/// <summary>
/// Class <c>TrainClassifier</c> contains all methods neccesary to recognize a face
/// </summary>
class TrainClassifier : IDisposable
{

    #region Variables
    private List<Image<Gray, byte>> trainingImages = new List<Image<Gray, byte>>();
    private List<string> names = new List<string>();
    private FaceRecognizer recognizer = new EigenFaceRecognizer(80, double.PositiveInfinity);
    private int eigenThreshold = 2000;
    private float eigenDistance = 0;
    public string recognizerType = "EMGU.CV.EigenFaceRecognizer";
    private string eigenLabel;
    #endregion

    #region Methods 
    /// <summary>
    /// This Method recognizes a grayscale image using the EigenFaceRecognizer
    /// </summary>
    /// <param name="Input_image"></param>
    /// <returns></returns>
    public string Recognize(Image<Gray, byte> Input_image, int Eigen_Thresh = -1)
    {
        //initailize and fill up listOfInts
        Globals.listOfInts = new List<int>();
        for (int i = 0; i < Globals.numLabels; i++)
        {
            Globals.listOfInts.Add(i);
        }
        //check if eigen xml file already exists
        if (File.Exists("../../../UniFCR_Controller/eigen.xml"))
        {
            Globals.fileSaved = true;

        }
       
        //if eigen xml file does not exist, then train the recognizer, save the file, and set the fielSaved var to false
        if (Globals.fileSaved == false)
        {
            recognizer.Train(Globals.trainingImages.ToArray(), Globals.listOfInts.ToArray());
            this.saveEigenRecognizer("../../../UniFCR_Controller/eigen.xml");
            Globals.fileSaved = true;
        }
        //if eigen file already exists then load and use it
        else
        {
            this.loadEigenRecognizer("../../../UniFCR_Controller/eigen.xml");
        }
        FaceRecognizer.PredictionResult ER = recognizer.Predict(Input_image);

        if (ER.Label == -1)
        {
            eigenLabel = "Unknown";
            eigenDistance = 0;
            return eigenLabel;
        }
        else
        {
            eigenLabel = Globals.studentNames[ER.Label];
            Globals.numIndex = Globals.studentNumbers[ER.Label];
            eigenDistance = (float)ER.Distance;
            if (Eigen_Thresh > -1) eigenThreshold = Eigen_Thresh;

            if (eigenDistance > eigenThreshold)
            {
                return eigenLabel;
            } else
            {
                return "Unknown";
            }
        }
    }

    /// <summary>
    /// Sets the eigenThreshold
    /// </summary>
    public int setEigenThreshold
    {
        set
        {
            eigenThreshold = value;
        }
    }

    /// <summary>
    /// Saves the trained Eigen Recogniser to specified location
    /// </summary>
    /// <param name="filename"></param>
    public void saveEigenRecognizer(string filename)
    {
        recognizer.Save(filename);

        string path = Path.GetDirectoryName(filename);
        FileStream labelData = File.OpenWrite(path + "/Labels.xml");
        using (XmlWriter writer = XmlWriter.Create(labelData))
        {
            writer.WriteStartDocument();
            writer.WriteStartElement("Labels_For_Recognizer_sequential");
            for (int i = 0; i < names.Count; i++)
            {
                writer.WriteStartElement("LABEL");
                writer.WriteElementString("POS", i.ToString());
                writer.WriteElementString("NAME", names[i]);
                writer.WriteEndElement();
            }

            writer.WriteEndElement();
            writer.WriteEndDocument();
        }
        labelData.Close();
    }

    /// <summary>
    /// This Method loads the eigenRecognizer
    /// </summary>
    /// <param name="filename"></param>
    public void loadEigenRecognizer(string filename)
    {

        string ext = Path.GetExtension(filename);
        switch (ext)
        {
            case (".LBPH"):
                recognizerType = "EMGU.CV.LBPHFaceRecognizer";
                recognizer = new LBPHFaceRecognizer(1, 8, 8, 8, 100);//50
                break;
            case (".FFR"):
                recognizerType = "EMGU.CV.FisherFaceRecognizer";
                recognizer = new FisherFaceRecognizer(0, 3500);//4000
                break;
            case (".EFR"):
                recognizerType = "EMGU.CV.EigenFaceRecognizer";
                recognizer = new EigenFaceRecognizer(80, double.PositiveInfinity);
                break;
        }

        recognizer.Load(filename);

        string direct = Path.GetDirectoryName(filename);
        names.Clear();
        if (File.Exists(direct + "/Labels.xml"))
        {
            FileStream filestream = File.OpenRead(direct + "/Labels.xml");
            long filelength = filestream.Length;
            byte[] xmlBytes = new byte[filelength];
            filestream.Read(xmlBytes, 0, (int)filelength);
            filestream.Close();

            MemoryStream xmlStream = new MemoryStream(xmlBytes);

            using (XmlReader xmlreader = XmlTextReader.Create(xmlStream))
            {
                while (xmlreader.Read())
                {
                    if (xmlreader.IsStartElement())
                    {
                        switch (xmlreader.Name)
                        {
                            case "NAME":
                                if (xmlreader.Read())
                                {
                                    names.Add(xmlreader.Value.Trim());
                                }
                                break;
                        }
                    }
                }
            }
        }
    }

    /// <summary>
    /// This method disposes the recognizer
    /// </summary>
    public void Dispose()
    {
        recognizer.Dispose();
        trainingImages = null;
        names = null;
        GC.Collect();
    }

    #endregion
}
