using System;
using System.Collections.Generic;
using System.Windows.Forms;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NationalInstruments.ModularInstruments.NIRfsg;
using NationalInstruments.ModularInstruments.NIRfsgPlayback;

namespace L2CapstoneProject
{
    class SimulatedSequencedBeamformer : ISequencedBeamformer
    {

        public double TimePerStep { get; set; }

        public double IQRate { get; set; }

        public double CenterFrequency { get; set; }

        List<string> waveformNamesLoaded;

        public SimulatedSequencedBeamformer(double timePerStep)
            {
            TimePerStep = timePerStep;
            IQRate = 1E6;
            CenterFrequency = 1E9;
            }

        NIRfsg session;
        readonly string VSGName = "";
        Dictionary<string, List<PhaseAmplitudeOffset>> sequences;
        IntPtr sessionPtr;

        public void Connect()
        {
            PopulateSequenceList();
            waveformNamesLoaded = new List<string>();
            session = new NIRfsg(VSGName, false, true);
            session.RF.Frequency = CenterFrequency; 
            session.Arb.GenerationMode = RfsgWaveformGenerationMode.ArbitraryWaveform;
            session.Arb.IQRate = IQRate;
        }

        public void Disconnect()
        {
            session.Dispose();
        }

        public void LoadSequence(string sequenceName)
        {
            List<PhaseAmplitudeOffset> sequenceToLoad;
            if (sequences.TryGetValue(sequenceName,out sequenceToLoad))
            {
                var waveform = CreateWaveform(sequenceToLoad);
                session.Arb.WriteWaveform(sequenceName, waveform.IData, waveform.QData);
            }
            else
            {
                MessageBox.Show("no waveform found by this name");
            }

        }

        public void LoadSequence(string SequenceName, List<PhaseAmplitudeOffset> offsets)
        {
            var waveform = CreateWaveform(offsets);
            session.Arb.WriteWaveform(SequenceName, waveform.IData, waveform.QData);
        }

        private IQWaveform CreateWaveform(List<PhaseAmplitudeOffset> offsets)
        {
            int numberOfSamplesPerStep = (int)(TimePerStep * IQRate);//(time/step)*iqrate
            double[] iData = new double[numberOfSamplesPerStep*offsets.Count];
            double[] qData = new double[numberOfSamplesPerStep*offsets.Count];
            var j = 0;
            foreach(var offset in offsets)
            {
                double iValue = Decimal.ToDouble(offset.Amplitude) * Math.Sin(Decimal.ToDouble(offset.Phase));
                double qValue = Decimal.ToDouble(offset.Amplitude) * Math.Cos(Decimal.ToDouble(offset.Phase));
                for (int k=j*numberOfSamplesPerStep; k<(j+1)*numberOfSamplesPerStep; k++)
                {
                    iData[k] = iValue;
                    qData[k] = qValue;
                }

                j++;
            }
            return new IQWaveform(iData, qData);
        }

        public void InitiateSequence(string sequenceName)
        {
            if (waveformNamesLoaded.Contains(sequenceName))
                {
                session.Arb.SelectedWaveform = sequenceName;
                session.Initiate();
                }
            else
            {
                MessageBox.Show("no loaded waveforms found by this name");
            }
        }

        public void AbortSequence()
        {
            session.Abort();
        }


        private void PopulateSequenceList()
        {
            sequences = new Dictionary<string, List<PhaseAmplitudeOffset>>()
            {
                  { "sequence1", new List<PhaseAmplitudeOffset>() { new PhaseAmplitudeOffset(0,0),
                                                                  new PhaseAmplitudeOffset(0,-3),
                                                                  new PhaseAmplitudeOffset(0,-6),
                                                                  new PhaseAmplitudeOffset(0,-9),
                                                                  new PhaseAmplitudeOffset(90,0),
                                                                  new PhaseAmplitudeOffset(90,-3),
                                                                  new PhaseAmplitudeOffset(90,-6),
                                                                  new PhaseAmplitudeOffset(90,-9) } },
                  { "sequence2", new List<PhaseAmplitudeOffset>(){new PhaseAmplitudeOffset(0, 0),
                                                                  new PhaseAmplitudeOffset(0, 1),
                                                                  new PhaseAmplitudeOffset(0, 2),
                                                                  new PhaseAmplitudeOffset(0, 3),
                                                                  new PhaseAmplitudeOffset(30, 0),
                                                                  new PhaseAmplitudeOffset(30, 1),
                                                                  new PhaseAmplitudeOffset(30, 2),
                                                                  new PhaseAmplitudeOffset(30, 3) } }
            };

        }



    }
}