using System;
using System.Collections.Generic;

namespace OpenPetz
{
    public partial class BallzModel
    {
//      class for a single frame of an animation
        unsafe public class Frame {
            List<KeyValuePair<int /*ball number*/, Tuple<BallOrientation3D, int> /*orientation, sizeoffset*/>> BallzData;

            public Frame(BdtFrame* pBallArray, int numBallz) {
                BallzData = new List<KeyValuePair<int, Tuple<BallOrientation3D, int>>>();
                BdtFrame.BdtBallSizeOverrideArray.BdtFrameBallSizeOverride* pOverrideArray = &pBallArray->SizeArray.SizeOverrides;  //  temporary ball size override lookup
                Dictionary<int, int> sizeOverrides = new Dictionary<int, int>();
                for (int i = 0; i < pBallArray->SizeArray.ArrayLength; i++) {
                    sizeOverrides.Add(pOverrideArray->Ball, pOverrideArray->SizeDiff);
                    pOverrideArray++;
                };

//              for each ball in the frame, parse its position/rotation/size offset
                XPointRot3_16* pBall = &pBallArray->Ballz;

                for (int i = 0; i < numBallz; i++) {
                    BallzData.Add(new(i, new(new BallOrientation3D(pBall), sizeOverrides.ContainsKey(i) ? sizeOverrides[i] : 0)));
                    pBall++;
                };
            }

            public BallOrientation3D BallOrientation(int ballNumber) {
                return BallzData.Find(x => x.Key == ballNumber).Value.Item1;
            }
            public BallOrientation3D BallOrientation(long ballNumber) {
                return BallzData.Find(x => x.Key == (int)ballNumber).Value.Item1;
            }

            public int BallSizeOffset(int ballNumber) {
                return BallzData.Find(x => x.Key == ballNumber).Value.Item2;
            }
            public long BallSizeOffset(long ballNumber) {
                return BallzData.Find(x => x.Key == (int)ballNumber).Value.Item2;
            }
        };
    };
};