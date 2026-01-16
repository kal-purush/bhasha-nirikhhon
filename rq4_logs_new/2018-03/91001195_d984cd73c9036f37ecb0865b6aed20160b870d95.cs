using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Face;
using KinectUnifier;
using LuxandFaceLib;

namespace KinectFaceTracker
{
    public class KinectFaceTracker : IDisposable
    {
        public LuxandFacePipeline FacePipeline => _facePipeline;
        public IReadOnlyDictionary<long, TrackingStatus> TrackedFaces => _facePipeline.TrackedFaces;

        public FaceDatabase<byte[]> FaceDatabase
        {
            get => _facePipeline.FaceDb;
            set => _facePipeline.FaceDb = value;
        }
        public bool IsRunning => _kinect.IsRunning;

        private readonly IKinect _kinect;
        private IMultiManager _multiManager;
        private readonly ICoordinateMapper _coordinateMapper;

        private readonly LuxandFacePipeline _facePipeline;

        public event EventHandler<FrameArrivedEventArgs> FrameArrived;

        public KinectFaceTracker(LuxandFacePipeline facePipeline, IKinect kinect)
        {
            _kinect = kinect;
            _facePipeline = facePipeline;
            _coordinateMapper = _kinect.CoordinateMapper;
        }

        public void Start()
        {
            if (_multiManager == null)
            {
                _multiManager = _kinect.OpenMultiManager(MultiFrameTypes.Body | MultiFrameTypes.Color, true);
                _multiManager.MultiFrameArrived += MultiManagerOnMultiFrameArrived;
            }
            _kinect.Open();
        }

        public void Stop()
        {
            _kinect.Close();
        }

        private async void MultiManagerOnMultiFrameArrived(object sender, MultiFrameReadyEventArgs e)
        {
            var multiFrame = e.MultiFrame;
            if (multiFrame == null) return;

            //if (_facePipeline.Completion.IsCompleted) throw _facePipeline.Completion.Result.Exception.InnerException;

            using (var colorFrame = multiFrame.ColorFrame)
            using (var bodyFrame = multiFrame.BodyFrame)
            {
                if (colorFrame == null || bodyFrame == null) return;
                var faceLocations = await _facePipeline.LocateFacesAsync(colorFrame, bodyFrame, _coordinateMapper);
                FrameArrived?.Invoke(this, new FrameArrivedEventArgs(faceLocations));
            }
            multiFrame.Dispose();
        }

        public void Dispose()
        {
            _multiManager.Dispose();
        }
    }

    public class FrameArrivedEventArgs : EventArgs
    {
        public FaceLocationResult FaceLocationResult { get; }

        public FrameArrivedEventArgs(FaceLocationResult flr)
        {
            FaceLocationResult = flr;
        }
    }
}

    