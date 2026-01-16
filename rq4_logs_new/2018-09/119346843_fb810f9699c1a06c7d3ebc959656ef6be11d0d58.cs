using GameLib.Utilities.Network.Streamable;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GameLib.Utilities {
	public enum BattleMapDirection {
		POSITIVE_ROW = 0b0001,
		POSITIVE_COL = 0b0010,
		NEGATIVE_ROW = 0b0100,
		NEGATIVE_COL = 0b1000
	}

	public enum CharacterAction {
		CREATE_ASPECT = 0b001,
		ATTACK = 0b010,
		HINDER = 0b100
	}

	public enum CheckResult {
		FAIL,
		TIE,
		SUCCEED,
		SUCCEED_WITH_STYLE
	}

	public struct Vec2 : IStreamable {
		public float X, Y;

		public Vec2(float x, float y) {
			X = x; Y = y;
		}

		public void ReadFrom(IDataInputStream stream) {
			X = stream.ReadSingle();
			Y = stream.ReadSingle();
		}

		public void WriteTo(IDataOutputStream stream) {
			stream.WriteSingle(X);
			stream.WriteSingle(Y);
		}
	}

	public struct Vec3 : IStreamable {
		public float X, Y, Z;

		public Vec3(float x, float y, float z) {
			X = x; Y = y; Z = z;
		}

		public void ReadFrom(IDataInputStream stream) {
			X = stream.ReadSingle();
			Y = stream.ReadSingle();
			Z = stream.ReadSingle();
		}

		public void WriteTo(IDataOutputStream stream) {
			stream.WriteSingle(X);
			stream.WriteSingle(Y);
			stream.WriteSingle(Z);
		}
	}

	public struct Vec4 : IStreamable {
		public float X, Y, Z, W;

		public Vec4(float x, float y, float z, float w) {
			X = x; Y = y; Z = z; W = w;
		}

		public void ReadFrom(IDataInputStream stream) {
			X = stream.ReadSingle();
			Y = stream.ReadSingle();
			Z = stream.ReadSingle();
			W = stream.ReadSingle();
		}

		public void WriteTo(IDataOutputStream stream) {
			stream.WriteSingle(X);
			stream.WriteSingle(Y);
			stream.WriteSingle(Z);
			stream.WriteSingle(W);
		}
	}

	public struct Range : IStreamable {
		public bool lowOpen;
		public float low;

		public bool highOpen;
		public float high;

		public Range(float greaterEqual, float less) {
			lowOpen = false;
			highOpen = true;
			low = greaterEqual;
			high = less;
		}

		public bool InRange(float num) {
			bool ret = true;
			ret &= num > low || (!lowOpen && num == low);
			ret &= num < high || (!highOpen && num == high);
			return ret;
		}

		public bool OutOfRange(float num) {
			return !this.InRange(num);
		}

		public void ReadFrom(IDataInputStream stream) {
			lowOpen = stream.ReadBoolean();
			low = stream.ReadSingle();
			highOpen = stream.ReadBoolean();
			high = stream.ReadSingle();
		}

		public void WriteTo(IDataOutputStream stream) {
			stream.WriteBoolean(lowOpen);
			stream.WriteSingle(low);
			stream.WriteBoolean(highOpen);
			stream.WriteSingle(high);
		}
	}

	public sealed class CharacterView : IStreamable {
		public string id = "";
		public string battle = "";
		public string story = "";

		public void ReadFrom(IDataInputStream stream) {
			id = stream.ReadString();
			story = stream.ReadString();
			battle = stream.ReadString();
		}

		public void WriteTo(IDataOutputStream stream) {
			stream.WriteString(id);
			stream.WriteString(story);
			stream.WriteString(battle);
		}
	}

	public struct Layout : IStreamable {
		public static readonly Layout INIT = new Layout(new Vec3(0, 0, 0), 0, new Vec2(1, 1));

		public Vec3 pos;
		public float rot;
		public Vec2 sca;

		public Layout(Vec3 pos, float rot, Vec2 sca) {
			this.pos = pos;
			this.rot = rot;
			this.sca = sca;
		}

		public void ReadFrom(IDataInputStream stream) {
			pos.ReadFrom(stream);
			rot = stream.ReadSingle();
			sca.ReadFrom(stream);
		}

		public void WriteTo(IDataOutputStream stream) {
			pos.WriteTo(stream);
			stream.WriteSingle(rot);
			sca.WriteTo(stream);
		}
	}

	public struct GridPos : IStreamable {
		public int row;
		public int col;
		public bool highland;

		public override int GetHashCode() {
			int hash = ((row << 8) & 0xFF00) | (col & 0xFF);
			return highland ? (hash ^ 0xFFFF) : hash;
		}
		
		public void ReadFrom(IDataInputStream stream) {
			row = stream.ReadInt32();
			col = stream.ReadInt32();
			highland = stream.ReadBoolean();
		}

		public void WriteTo(IDataOutputStream stream) {
			stream.WriteInt32(row);
			stream.WriteInt32(col);
			stream.WriteBoolean(highland);
		}
	}

	public struct CameraEffect : IStreamable {
		public enum AnimateType {
			None, Shake
		}

		public static readonly CameraEffect INIT = new CameraEffect(AnimateType.None);

		public AnimateType animation;

		public CameraEffect(AnimateType animation) {
			this.animation = animation;
		}

		public void WriteTo(IDataOutputStream stream) {
			stream.WriteByte((byte)animation);
		}

		public void ReadFrom(IDataInputStream stream) {
			animation = (CameraEffect.AnimateType)stream.ReadByte();
		}
	}

	public struct CharacterViewEffect : IStreamable {
		public enum AnimateType {
			None, Shake
		}

		public static readonly CharacterViewEffect INIT = new CharacterViewEffect(new Vec4(1, 1, 1, 1), AnimateType.None);

		public Vec4 tint;
		public AnimateType animation;

		public CharacterViewEffect(Vec4 tint, AnimateType animation) {
			this.tint = tint;
			this.animation = animation;
		}

		public void WriteTo(IDataOutputStream stream) {
			stream.WriteByte((byte)animation);
			tint.WriteTo(stream);
		}

		public void ReadFrom(IDataInputStream stream) {
			animation = (CharacterViewEffect.AnimateType)stream.ReadByte();
			tint.ReadFrom(stream);
		}
	}

	public struct PortraitStyle : IStreamable {
		public static readonly PortraitStyle INIT = new PortraitStyle(0, 0);

		public int action;
		public int emotion;

		public PortraitStyle(int action, int emotion) {
			this.action = action;
			this.emotion = emotion;
		}

		public void ReadFrom(IDataInputStream stream) {
			action = stream.ReadInt32();
			emotion = stream.ReadInt32();
		}

		public void WriteTo(IDataOutputStream stream) {
			stream.WriteInt32(action);
			stream.WriteInt32(emotion);
		}
	}
}