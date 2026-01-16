using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using KLibrary.Labs.ObservableModel;
using Microsoft.Kinect;

namespace DepthMeasure
{
    public class AppModel
    {
        const double Frequency = 30;
        static readonly TimeSpan FramesInterval = TimeSpan.FromSeconds(1 / Frequency);

        public AppModel()
        {
            var kinect = new AsyncKinectManager();
            kinect.SensorConnected
                .Subscribe(sensor =>
                {
                    sensor.ColorStream.Enable(ColorImageFormat.RgbResolution640x480Fps30);
                    sensor.DepthStream.Enable(DepthImageFormat.Resolution640x480Fps30);

                    try
                    {
                        sensor.Start();
                    }
                    catch (Exception ex)
                    {
                        // センサーが他のプロセスに既に使用されている場合に発生します。
                        Debug.WriteLine(ex);
                    }
                });
            kinect.SensorDisconnected
                .Subscribe(sensor => sensor.Stop());
            kinect.Initialize();

            var frameData = Observable.Interval(FramesInterval)
                .Select(_ => new
                {
                    ColorData = kinect.Sensor.Value.GetColorData(FramesInterval),
                    DepthData = kinect.Sensor.Value.GetDepthData(FramesInterval),
                })
                .ToGetOnly(null);
        }
    }
}