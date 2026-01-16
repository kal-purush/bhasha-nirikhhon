using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Styx.Common;

namespace bot
{
    public enum FunctionWow
    {
        ClntObjMgrGetActivePlayer = 0x39B615,
        ClntObjMgrGetActivePlayerObj = 0x4FC6,
        FrameScript_ExecuteBuffer = 0x4fd12,
        Spell_C_HandleTerrainClick = 0x38f129,
        FrameScript__GetLocalizedText = 0x414267,
        IsOutdoors = 0x414b53,
        UnitCanAttack = 0x41ad3c,
        CGUnit_C__InitializeTrackingState = 0x41fb57,
        CGWorldFrame__Intersect = 0x5eef7b,
        CGUnit_C__Interact = 0x8D01D0,
    }

    public enum ClickToMove
    {
        CTM = 0x420543,
        CTM_PUSH = 0xD0EEBC,
        CTM_X = 0xD0EF2C,
        CTM_Y = CTM_X + 4,
        CTM_Z = CTM_Y + 4,
    }
    
    public enum ClickType
    {
        FaceTarget = 0x1,
        Face = 0x2,
        StopThrowsException = 0x3,
        Move = 0x4,
        NpcInteract = 0x5,
        Loot = 0x6,
        ObjInteract = 0x7,
        FaceOther = 0x8,
        Skin = 0x9,
        AttackPosition = 0xa,
        AttackGuid = 0xb,
        ConstantFace = 0xc,
        None = 0xd,
        Attack = 0x10,
        Idle = 0x13,
    }



    public static class WorldClick
    {
        public static void ClickTo(float x, float y, float z, ulong guid, ClickType action, float precision)
        {
            if (Math.Abs(x) < 0.1 && Math.Abs(y) < 0.1 && (Math.Abs(z) < 0.1 && (long)guid == 0L))
                return;
            //память для 3х координат
            var positionAddress = ProcessMemory.AllocateMemory(3 * sizeof(float));
            //guid типа ulong в 8 байт
            var guidAddress = ProcessMemory.AllocateMemory(sizeof(ulong));
            //значение точности, до которой продолжать движение, я беру 0.5f
            var precisionAddress = ProcessMemory.AllocateMemory(sizeof(float));
            if ((uint)positionAddress <= 0U || (uint)guidAddress <= 0U || (uint)precisionAddress <= 0U)
                return;
            ProcessMemory.Write(guidAddress, guid);
            ProcessMemory.Write(precisionAddress, precision);
            ProcessMemory.Write(positionAddress, x);
            ProcessMemory.Write(positionAddress + IntPtr.Size, y);
            ProcessMemory.Write(positionAddress + IntPtr.Size * 2, z);
            var asm = new[]
                        {
                        "call " + ProcessMemory.GetAbsolute((uint)FunctionWow.ClntObjMgrGetActivePlayer ),
                         //Проверка на наличие активного игрока
                        "test eax, eax",
                        "je @out",
                         //Получаем указатель на объект - понадобится ниже
                        "call " + ProcessMemory.GetAbsolute((uint)FunctionWow.ClntObjMgrGetActivePlayerObj),
                        "test eax, eax",
                        "je @out",
                        "mov edx, [" + precisionAddress + "]",
                        "push edx",
                        "push " + positionAddress,
                        "push " + guidAddress,
                        "push " + (int)action,
                        "mov ecx, eax",
                        //Вызываем ClickToMove()
                        "call " + ProcessMemory.GetAbsolute((int)ClickToMove.CTM),
                        "@out:",
                        "retn"
                        };
            HookManager.InjectAndExecute(asm);
            ProcessMemory.FreeMemory(positionAddress);
            ProcessMemory.FreeMemory(guidAddress);
            ProcessMemory.FreeMemory(precisionAddress);
        }

        public static ClickToMove GetClickTypePush()
        {
            return (ClickToMove)ProcessMemory.Read((IntPtr)ClickToMove.CTM_PUSH);
        }

        public static Vector3 GetClickPosition()
        {
            return new Vector3(
                    ProcessMemory.Read((IntPtr)ClickToMove.CTM_X),
                    ProcessMemory.Read((IntPtr)ClickToMove.CTM_Y),
                    ProcessMemory.Read((IntPtr)ClickToMove.CTM_Z));
        }
    }
}