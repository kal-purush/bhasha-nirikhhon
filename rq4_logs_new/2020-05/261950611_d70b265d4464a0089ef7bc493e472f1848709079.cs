using System;
using System.Xml;
using System.Xml.Serialization;
using System.IO;

[XmlRootAttribute("Job", IsNullable = false)]
public class Job
{
    public Config Configuration;
    public Data DataPath;
}

public class Config
{
    //default type
    [XmlElementAttribute(IsNullable = false)]
    public bool UseShell;
    public int Size;
    public string EncodingString;
}

public class Data
{
    public string Input;
    public string Output;
}

public class Test
{
    public static void Main()
    {
        // Read and write Job
        Test t = new Test();
        t.CreateJob("JobConfig.xml");
        //t.ReadJob_deserialization("JobConfig.xml");
        t.ReadJob_eachline("JobConfig.xml");
    }

    private void CreateJob(string filename)
    {
        // create XmlSerializer
        XmlSerializer serializer =
        new XmlSerializer(typeof(Job));
        TextWriter writer = new StreamWriter(filename);
        Job job = new Job();

        // Create an config class
        Config configuration = new Config();
        configuration.UseShell = true;
        configuration.Size = 4;
        configuration.EncodingString = "UTF-8";
        // Create an Data class
        Data datapath = new Data();
        datapath.Input = "d:/test/data";
        datapath.Output = "d:/test/result";
        // Set Data and Config to Job
        job.Configuration = configuration;
        job.DataPath = datapath;
        serializer.Serialize(writer, job);
        writer.Close();
    }

    protected Job ReadJob_deserialization(string filename)
    {
        // Create an XmlSerializer
        XmlSerializer serializer = new XmlSerializer(typeof(Job));
        //defatul unknown function in microsoft doc.
        serializer.UnknownNode += new
        XmlNodeEventHandler(serializer_UnknownNode);
        serializer.UnknownAttribute += new
        XmlAttributeEventHandler(serializer_UnknownAttribute);

        //read the XML document.
        FileStream fs = new FileStream(filename, FileMode.Open);
        // Declare an object variable of the type to be deserialized.
        Job job;
        /* Use the Deserialize method to restore the object's state with
        data from the XML document. */
        job = (Job)serializer.Deserialize(fs);
        // Read the configuration.
        Config configuration = job.Configuration;
        Console.WriteLine("\t" +
            configuration.UseShell + "\t" +
            configuration.Size + "\t" +
            configuration.EncodingString);
        // Read the datapath.
        Data datapath = job.DataPath;
        Console.WriteLine("\t" +
        datapath.Input + "\t" +
        datapath.Output);
        return job;
    }

    protected Job ReadJob_eachline(string filename)
    {
        //read xml file
        XmlTextReader reader = new XmlTextReader(filename);
        //build new empty object
        Job job = new Job();
        Config configuration = new Config();
        Data datapath = new Data();
        job.Configuration = configuration;
        job.DataPath = datapath;
        //read the whole file
        while (reader.Read())
        {
            if (reader.NodeType == XmlNodeType.Element)
            {
                //use if to parse different element
                if (reader.Name == "UseShell")
                {
                    job.Configuration.UseShell = reader.ReadElementContentAsBoolean();
                }
                if (reader.Name == "Size")
                {
                    job.Configuration.Size = reader.ReadElementContentAsInt();
                }
                if (reader.Name == "EncodingString")
                {
                    job.Configuration.EncodingString = reader.ReadElementContentAsString().Trim();
                }
                if (reader.Name == "Input")
                {
                    job.DataPath.Input = reader.ReadElementContentAsString().Trim();
                }
                if (reader.Name == "Output")
                {
                    job.DataPath.Output = reader.ReadElementContentAsString().Trim();
                }
            }
        }
        //test for output
        Console.WriteLine("\t" +
        configuration.UseShell + "\t" +
        configuration.Size + "\t" +
        configuration.EncodingString);
        Console.WriteLine("\t" +
        job.DataPath.Input + "\t" +
        job.DataPath.Output);
        return job;
    }
    private void serializer_UnknownNode
    (object sender, XmlNodeEventArgs e)
    {
        Console.WriteLine("Unknown Node:" + e.Name + "\t" + e.Text);
    }

    private void serializer_UnknownAttribute
    (object sender, XmlAttributeEventArgs e)
    {
        System.Xml.XmlAttribute attr = e.Attr;
        Console.WriteLine("Unknown attribute " +
        attr.Name + "='" + attr.Value + "'");
    }
}