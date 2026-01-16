using NationalInstruments.ModularInstruments.NIRfsg;
using NationalInstruments;
using System;
using System.Collections.Generic;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace L2CapstoneProject
{
    class SimulatedSteppedBF : IDynamicBeamformer
    {
        NIRfsg _rfsgSession;
        frmBeamformerPavtController beamformerPavt = new frmBeamformerPavtController();
        //List<PhaseAmplitudeOffset> offsets;

        public void Connect()
        {
            string resourceName;
            double frequency, frequencyOffset, power, actualIQRate;
            //decimal phase, phaseOffset, amplitude, amplitudeOffset;
            int numSamples = 100; //use waveform quantum to find num samples instead?
            double[] iData, qData;
            ComplexWaveform<ComplexDouble> IQData = new ComplexWaveform<ComplexDouble>(numSamples);
            List<PhaseAmplitudeOffset> offsets = new List<PhaseAmplitudeOffset>();

            try
            {
                
                resourceName = beamformerPavt.rfsgNameComboBox.Text;
                frequency = (double)beamformerPavt.frequencyNumeric.Value;
                power = (double)beamformerPavt.powerLevelNumeric.Value;

                //initialize rfsg session
                _rfsgSession = new NIRfsg(resourceName, true, false);
                //subscribe to rfsg warnings
                _rfsgSession.DriverOperation.Warning += new EventHandler<RfsgWarningEventArgs>(DriverOperation_Warning);

                //configure generator
                _rfsgSession.RF.Configure(frequency, power);
                _rfsgSession.Arb.GenerationMode = RfsgWaveformGenerationMode.ArbitraryWaveform;
                _rfsgSession.Arb.IQRate = 50e6;
                actualIQRate = _rfsgSession.Arb.IQRate;
                frequencyOffset = actualIQRate / numSamples;
                _rfsgSession.Arb.SignalBandwidth = 2 * frequencyOffset;

                iData = new double[numSamples];
                qData = new double[numSamples];
                iData = sinePattern(numSamples, 1.0, 0.0, 1.0);
                qData = sinePattern(numSamples, 1.0, 0.0, 1.0);

                //generate a cw to stimulate dut 
                _rfsgSession.Arb.WriteWaveform("", iData, qData);
                _rfsgSession.Initiate();
                System.Threading.Thread.Sleep(100);
                _rfsgSession.Abort();

                /*
                  rest of code to write offsets
                 

                 PrecisionTimeSpan dt = PrecisionTimeSpan.FromSeconds(1 / actualIQRate);
                 IQData.PrecisionTiming = PrecisionWaveformTiming.CreateWithRegularInterval(dt);
                _rfsgSession.Arb.WriteWaveform("", createWaveform(offsets));
                _rfsgSession.Initiate();

                beamformerPavt.btnStop.Focus();
                */

            }
            catch (Exception ex)
            {
                MessageBox.Show("Error in Connect()\n" + ex.Message);
            }

                throw new NotImplementedException();
        }

        public void Disconnect()
        {
            _rfsgSession.Abort();
            
            throw new NotImplementedException();
        }

        public void LoadOffset(PhaseAmplitudeOffset offset)
        {
            //get phase and amplitude
            _rfsgSession.RF.PhaseOffset = (double)offset.Phase;
            _rfsgSession.RF.PowerLevel = (double)offset.Amplitude;

            throw new NotImplementedException();
        }

        void DriverOperation_Warning(object sender, RfsgWarningEventArgs e)
        {
            //errorTextBox.Text = e.Message;
            MessageBox.Show(e.Message);
        }

        static double[] sinePattern(int numSamples, double amplitude, double phaseDegrees, double numCycles)
        {
            double[] sineArray = new double[numSamples];
            for (int i = 0; i < numSamples; i++)
            {
                sineArray[i] = amplitude * Math.Sin(2 * Math.PI * i * numCycles / numSamples + Math.PI * phaseDegrees / 180); //m(t) = Asin(2*pi*f*t + theta)
            }
            return sineArray;
        }

        ComplexWaveform<ComplexDouble> createWaveform(List<PhaseAmplitudeOffset> offsets)
        {
            ComplexWaveform<ComplexDouble> complexWaveform;
            ComplexDouble[] IQData = new ComplexDouble[offsets.Count];

            for (int i = 0; i < offsets.Count; i++)
            {
                IQData[i] = ComplexDouble.FromPolar((double)offsets[i].Amplitude, (double)offsets[i].Phase);
            }

            complexWaveform = ComplexWaveform<ComplexDouble>.FromArray1D(IQData);

            return complexWaveform;
        }

    }
}