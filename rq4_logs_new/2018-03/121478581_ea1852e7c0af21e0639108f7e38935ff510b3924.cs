using System;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.ComponentModel.Composition;

namespace ScalesHubPlugin
{    
    /// <summary>
    /// Декодер по умолчанию
    /// </summary>
    [Export(typeof(IDataFrame))]
    [ExportMetadata("Name", "НВТ")]
    [ExportMetadata("Description", "Декодер по умолчанию")]
    public class NxDataFrame : IDataFrame
    {
        protected byte[] buffer;
        protected int offset;
        protected byte waiting_for;
        private Regex mask = new Regex(@"(?<value>\d+(\.|,)*\d*)", RegexOptions.Multiline | RegexOptions.CultureInvariant | RegexOptions.Compiled);
        public virtual bool Decode(byte[] data)
        {
            bool result = false;
            if (offset == buffer.Length)
            {
                offset = 0;
                waiting_for = 0xd;
            }
            for (int idx = 0; idx < data.Length;)
            {
                buffer[offset] = data[idx];
                if (data[idx] == waiting_for)
                {
                    String strBuffer = Encoding.ASCII.GetString(buffer, 0, offset + 1);
                    Match res = mask.Match(strBuffer);
                    String strState = String.Empty;
                    if (strBuffer.Length - 4 >= 0 && strBuffer.Length >= 1)
                        strState = strBuffer.Substring(strBuffer.Length - 4, 1);
                    else
                        strState = "@";
                    hStringState = strState;
                    System.Diagnostics.Trace.WriteLine("StringState = " + hStringState);
                    if (strState.Contains("A"))
                        hState |= NxDeviceState.DS_ZERO;
                    else
                        hState &= ~NxDeviceState.DS_ZERO;
                    if (strState.Contains("B"))
                        hState |= NxDeviceState.DS_STEADY;
                    else
                        hState &= ~NxDeviceState.DS_STEADY;
                    if (strState.Contains("D"))
                        hState |= NxDeviceState.DS_CALIBRATION;
                    else
                        hState &= ~NxDeviceState.DS_CALIBRATION;
                    if (strState.Contains("H"))
                        hState |= NxDeviceState.DS_MAX;
                    else
                        hState &= ~NxDeviceState.DS_MAX;
                    if (strState.Contains("C"))
                        hState = NxDeviceState.DS_STEADY | NxDeviceState.DS_ZERO;
                    if (strState.Contains("E"))
                        hState = NxDeviceState.DS_CALIBRATION | NxDeviceState.DS_ZERO;
                    if (strState.Contains("F"))
                        hState = NxDeviceState.DS_STEADY | NxDeviceState.DS_ZERO;
                    if (strState.Contains("G"))
                        hState = NxDeviceState.DS_STEADY | NxDeviceState.DS_ZERO | NxDeviceState.DS_CALIBRATION;
                    if (strState.Contains("I"))
                        hState = NxDeviceState.DS_MAX | NxDeviceState.DS_ZERO;
                    if (strState.Contains("J"))
                        hState = NxDeviceState.DS_MAX | NxDeviceState.DS_STEADY;
                    if (strState.Contains("K"))
                        hState = NxDeviceState.DS_MAX | NxDeviceState.DS_STEADY | NxDeviceState.DS_ZERO;
                    if (strState.Contains("L"))
                        hState = NxDeviceState.DS_MAX | NxDeviceState.DS_CALIBRATION;
                    if (strState.Contains("M"))
                        hState = NxDeviceState.DS_MAX | NxDeviceState.DS_CALIBRATION | NxDeviceState.DS_ZERO;
                    if (strState.Contains("N"))
                        hState = NxDeviceState.DS_MAX | NxDeviceState.DS_CALIBRATION | NxDeviceState.DS_STEADY;
                    if (strState.Contains("N"))
                        hState = NxDeviceState.DS_FULL;
                    if (strState.Contains("@") || String.IsNullOrWhiteSpace(strState))
                        hState = NxDeviceState.DS_NONE;
                    if (strBuffer.Length - 3 >= 0 && strBuffer.Length >= 2)
                        hMeasure = strBuffer.Substring(strBuffer.Length - 3, 2);
                    else
                        hMeasure = String.Empty;
                    offset = 0;
                    try
                    {
                        if (res.Success)
                        {
                            String src = res.Groups["value"].Value.Replace(',', '.');
                            hWeight = Double.Parse(src, CultureInfo.InvariantCulture);
                            if (strState.Contains("-"))
                                hWeight = -hWeight;
                            result = true;
                        }
                        else
                        {
                            hWeight = 0.0;
                            hState = NxDeviceState.DS_NONE;
                            result = false;
                        }
                    }
                    catch
                    {
                        offset = 0;
                        hWeight = 0.0;
                        result = false;
                    }
                }
                else
                {
                    offset++;
                }
                idx++;
            }
            return result;
        }
        protected NxDeviceState hState = NxDeviceState.DS_NONE;
        public NxDeviceState State
        {
            get { return hState; }
        }
        protected string hStringState = "@";
        public string StringState
        {
            get { return hStringState; }
        }
        protected double hWeight = 0.0;
        public double Weight
        {
            get { return hWeight; }
        }
        protected string hMeasure = "";
        public string Measure
        {
            get { return hMeasure; }
        }
        public override string ToString()
        {
            return String.Format("Вес {0:f3} {1}, состояние {2}", Weight, Measure, ((State & NxDeviceState.DS_STEADY) == NxDeviceState.DS_STEADY) ? "УСТ" : "ИЗМ");
        }
        public NxDataFrame()
        {
            buffer = new byte[128];
            offset = 0;
            waiting_for = 0xd;
        }
    } 
}