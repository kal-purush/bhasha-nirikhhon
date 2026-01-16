using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ColliderGUI {
	public delegate void OnRayCastLineSegment(int X, int Y, int Z);

	public static class RayCast {
		public static void RayCast3D(int X1, int Y1, int Z1, int X2, int Y2, int Z2, OnRayCastLineSegment OnRayCast) {
			int I, DX, DY, DZ, L, M, N, XInc, YInc, ZInc, Err1, Err2, DX2, DY2, DZ2;
			int PointX;
			int PointY;
			int PointZ;

			PointX = X1;
			PointY = Y1;
			PointZ = Z1;

			DX = X2 - X1;
			DY = Y2 - Y1;
			DZ = Z2 - Z1;
			XInc = (DX < 0) ? -1 : 1;
			L = Math.Abs(DX);
			YInc = (DY < 0) ? -1 : 1;
			M = Math.Abs(DY);
			ZInc = (DZ < 0) ? -1 : 1;
			N = Math.Abs(DZ);
			DX2 = L << 1;
			DY2 = M << 1;
			DZ2 = N << 1;

			if ((L >= M) && (L >= N)) {
				Err1 = DY2 - L;
				Err2 = DZ2 - L;
				for (I = 0; I < L; I++) {
					OnRayCast(PointX, PointY, PointZ);

					if (Err1 > 0) {
						PointY += YInc;
						Err1 -= DX2;
					}
					if (Err2 > 0) {
						PointZ += ZInc;
						Err2 -= DX2;
					}
					Err1 += DY2;
					Err2 += DZ2;
					PointX += XInc;
				}
			} else if ((M >= L) && (M >= N)) {
				Err1 = DX2 - M;
				Err2 = DZ2 - M;
				for (I = 0; I < M; I++) {
					OnRayCast(PointX, PointY, PointZ);

					if (Err1 > 0) {
						PointX += XInc;
						Err1 -= DY2;
					}
					if (Err2 > 0) {
						PointZ += ZInc;
						Err2 -= DY2;
					}
					Err1 += DX2;
					Err2 += DZ2;
					PointY += YInc;
				}
			} else {
				Err1 = DY2 - N;
				Err2 = DX2 - N;
				for (I = 0; I < N; I++) {
					OnRayCast(PointX, PointY, PointZ);

					if (Err1 > 0) {
						PointY += YInc;
						Err1 -= DZ2;
					}
					if (Err2 > 0) {
						PointX += XInc;
						Err2 -= DZ2;
					}
					Err1 += DY2;
					Err2 += DX2;
					PointZ += ZInc;
				}
			}

			OnRayCast(PointX, PointY, PointZ);
		}
	}
}