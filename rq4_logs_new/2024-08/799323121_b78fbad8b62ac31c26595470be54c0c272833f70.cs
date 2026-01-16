using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace TestAppAI
{
    public class DataCreationFactory
    {
        private string _path;
        public DataCreationFactory(string path) 
        {
            _path = path;
        }

        public List<TrainingData> GetTrainingData(byte[][] images, byte[] labels)
        {
            if (images.Length != labels.Length)
                throw new Exception("количество изображений не равно количествам названий");

            int length = images.Length;
            List<TrainingData> training = new List<TrainingData>();
            for (int i = 0; i < length; i++)
            {
                TrainingData data = new TrainingData();
                List<double> expectedAnswer = Enumerable.Range(0, 10).Select(x => 0.0).ToList();
                expectedAnswer[labels[i]] = 1;
                data.Inbox = images[i].Select(x => x == 0 ? 0.0 : 1.0).ToList();
                data.ExpectedAnswer = expectedAnswer;
                training.Add(data);
            }

            return training;
        }


        public byte[] ReadLabels(string name)
        {
            string path =  _path + name;
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read))
            using (var br = new BinaryReader(fs))
            {
                var magicNumber = br.ReadInt32();
                var numItems = br.ReadInt32();
                var bytes = br.ReadBytes(numItems);
                return bytes;
            }
        }

        public byte[][] ReadImages(string name)
        {
            string path = _path + name;

            const int imageSize = 28 * 28;
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read))
            using (var br = new BinaryReader(fs))
            {
                var magicNumber = br.ReadInt32();

                var numImages = IPAddress.NetworkToHostOrder(br.ReadInt32());
                var numRows = IPAddress.NetworkToHostOrder(br.ReadInt32());
                var numCols = IPAddress.NetworkToHostOrder(br.ReadInt32());

                var bytes = br.ReadBytes(imageSize * numImages);
                var images = new byte[numImages][];

                for (int i = 0; i < numImages; i++)
                {
                    images[i] = new byte[imageSize];
                    Array.Copy(bytes, i * imageSize, images[i], 0, imageSize);
                }

                return images;
            }
        }

    }
}