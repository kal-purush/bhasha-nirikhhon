using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SecureAutomatedSystem
{
    public class Emulator
    {
        private float delay;
        public float ProducerDelay
        {
            get
            {
                return delay;
            }
            set
            {
                if (value < 1)
                    delay = 1;
                else
                    delay = value;
            }
        }
        public bool SaveInDB { get; set; }
        public bool EncryptData { get; set; }
        public bool InWork { get; private set; }
        public Product CurrentProduct { get; private set; }
        public Product Benchmark { get; private set; }
        public Action ProductProduced;

        public Emulator() : this(1, false, false, new Product(10,10,10,10,10,10,10,10,10,10))
        { }

        public Emulator (float Delay, bool ProducerSaveInDB, bool ProducerEncryptData, Product ProductBenchmark)
        {
            ProducerDelay = Delay;
            SaveInDB = ProducerSaveInDB;
            EncryptData = ProducerEncryptData;
            Benchmark = ProductBenchmark;
        }

        public async void StartProducing()
        {
            InWork = true;
            Random rnd = new Random(DateTime.Now.Millisecond);
            while (InWork)
            {
                CurrentProduct = ProduceNewProduct(rnd);
                ProductProduced();
                await Task.Delay((int)ProducerDelay*1000);
            }
        }

        public void StopProducing()
        {
            InWork = false;
        }

        private Product ProduceNewProduct(Random RandomGeneratorVariable)
        {
            float OuterDiameter = Benchmark.OuterDiameter + (float)((RandomGeneratorVariable.Next(100)-50.0)/50.0) + (float)RandomGeneratorVariable.NextDouble()/2; 
            float OuterRadius = OuterDiameter / 2; 
            float InnerDiameter = Benchmark.InnerDiameter + (float)((RandomGeneratorVariable.Next(100) - 50.0) / 50.0) + (float)RandomGeneratorVariable.NextDouble()/2; 
            float InnerRadius = InnerDiameter / 2; 
            float OuterPairingRadius = Benchmark.OuterPairingRadius + (float)((RandomGeneratorVariable.Next(100) - 50.0) / 50.0) + (float)RandomGeneratorVariable.NextDouble()/2; 
            float WallThickness = Benchmark.WallThickness + (float)((RandomGeneratorVariable.Next(100) - 50.0) / 50.0) + (float)RandomGeneratorVariable.NextDouble()/2; 
            float TopThickness = Benchmark.TopThickness + (float)((RandomGeneratorVariable.Next(100) - 50.0) / 50.0) + (float)RandomGeneratorVariable.NextDouble()/2; 
            float BottomLowersectionHeight = Benchmark.BottomLowersectionHeight + (float)((RandomGeneratorVariable.Next(100) - 50.0) / 50.0) + (float)RandomGeneratorVariable.NextDouble()/2; 
            float TopDiameter = Benchmark.TopDiameter + (float)((RandomGeneratorVariable.Next(100) - 50.0) / 50.0) + (float)RandomGeneratorVariable.NextDouble()/2;
            float OuterPairingRadiusCyl = Benchmark.OuterPairingRadiusCyl + (float)((RandomGeneratorVariable.Next(100) - 50.0) / 50.0) + (float)RandomGeneratorVariable.NextDouble()/2;

            return new Product(OuterDiameter, OuterRadius, InnerDiameter, InnerRadius, OuterPairingRadius, WallThickness, TopThickness, BottomLowersectionHeight, TopDiameter, OuterPairingRadiusCyl);
        }

        private void SaveData(bool EncryptData)
        {

        }
    }
}