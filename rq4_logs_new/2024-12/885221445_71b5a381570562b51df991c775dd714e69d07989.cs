using Unity.Collections;
using Unity.Networking.Transport;
using UnityEngine;

public class NetPromotion : NetMessage
{
    public int teamId;
    public Vector2Int position;
    public ChessPieceType newPieceType;

    public NetPromotion()
    {
        Code = OpCode.PROMOTION;
    }
    public NetPromotion(DataStreamReader reader)
    {
        Code = OpCode.PROMOTION;
        DeSerialize(reader);
    }

    public override void Serialize(ref DataStreamWriter writer)
    {
        writer.WriteByte((byte)Code);
        writer.WriteInt(teamId);
        writer.WriteInt(position.x);
        writer.WriteInt(position.y);
        writer.WriteInt((int)newPieceType);
    }
    public override void DeSerialize(DataStreamReader reader)
    {
        teamId = reader.ReadInt();
        position = new Vector2Int(reader.ReadInt(), reader.ReadInt());
        newPieceType = (ChessPieceType)reader.ReadInt();
    }

    public override void ReceivedOnClient()
    {
        NetUtility.C_PROMOTION?.Invoke(this);
    }
    public override void ReceivedOnServer(NetworkConnection cnn)
    {
        NetUtility.S_PROMOTION?.Invoke(this, cnn);
    }
}