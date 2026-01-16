using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ColliderGUI {
	static class Voxels {
		static float VoxelSize;
		static int Width;
		static int Depth;
		static int Height;

		static bool[] VoxelArray;
		static bool Dirty;

		public static void Init(int Width, int Height, int Depth, float VoxelSize) {
			Voxels.Width = Width;
			Voxels.Height = Height;
			Voxels.Depth = Depth;
			Voxels.VoxelSize = VoxelSize;

			VoxelArray = new bool[Width * Height * Depth];
		}

		static int PosToIdx(int X, int Y, int Z) {
			return (Z * Width * Height) + (Y * Width) + X;
		}

		static void IdxToPos(int Idx, out int X, out int Y, out int Z) {
			Z = Idx / (Width * Height);
			Idx -= (Z * Width * Height);
			Y = Idx / Width;
			X = Idx % Width;
		}


		public static bool GetVoxel(int X, int Y, int Z) {
			if (X < 0 || X >= Width)
				return false;

			if (Y < 0 || Y >= Height)
				return false;

			if (Z < 0 || Z >= Depth)
				return false;

			return VoxelArray[PosToIdx(X, Y, Z)];
		}

		public static void SetVoxel(int X, int Y, int Z, bool Vox) {
			if (X < 0 || X >= Width)
				return;

			if (Y < 0 || Y >= Height)
				return;

			if (Z < 0 || Z >= Depth)
				return;

			VoxelArray[PosToIdx(X, Y, Z)] = Vox;
		}

		public static void Update() {
			if (!Dirty)
				return;
			Dirty = false;

			for (int i = 0; i < VoxelArray.Length; i++) {
				IdxToPos(i, out int X, out int Y, out int Z);
			}
		}

		static void EmitVoxel(int X, int Y, int Z) {

		}
	}
}