using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Reflection;

namespace DrAnalyzer.Analyzer
{
    public static class EnumExtensionMethods
    {
        public static string GetDescription(this Enum value)
        {
            Type type = value.GetType();
            string name = Enum.GetName(type, value);
            if (name != null)
            {
                FieldInfo field = type.GetField(name);
                if (field != null)
                {
                    DescriptionAttribute attr =
                           Attribute.GetCustomAttribute(field,
                             typeof(DescriptionAttribute)) as DescriptionAttribute;
                    if (attr != null)
                    {
                        return attr.Description;
                    }
                }
            }
            return null;
        }
    }

    [Flags]
    public enum ProcessAccessFlags : uint
    {
        All =                       0x001F0FFF,
        Terminate =                 0x00000001,
        CreateThread =              0x00000002,
        VirtualMemoryOperation =    0x00000008,
        VirtualMemoryRead =         0x00000010,
        VirtualMemoryWrite =        0x00000020,
        DuplicateHandle =           0x00000040,
        CreateProcess =             0x000000080,
        SetQuota =                  0x00000100,
        SetInformation =            0x00000200,
        QueryInformation =          0x00000400,
        QueryLimitedInformation =   0x00001000,
        Synchronize =               0x00100000
    }

    [Flags]
    public enum AllocationType
    {
        Commit =        0x1000,
        Reserve =       0x2000,
        Decommit =      0x4000,
        Release =       0x8000,
        Reset =         0x80000,
        Physical =      0x400000,
        TopDown =       0x100000,
        WriteWatch =    0x200000,
        LargePages =    0x20000000
    }

    [Flags]
    public enum MemoryProtection
    {
        Execute =                   0x10,
        ExecuteRead =               0x20,
        ExecuteReadWrite =          0x40,
        ExecuteWriteCopy =          0x80,
        NoAccess =                  0x01,
        ReadOnly =                  0x02,
        ReadWrite =                 0x04,
        WriteCopy =                 0x08,
        GuardModifierflag =         0x100,
        NoCacheModifierflag =       0x200,
        WriteCombineModifierflag =  0x400
    }

    [Flags]
    public enum GatherFuncType : UInt16
    {
        GatherUnknownFunc =     0x0000,
        GatherFileFunc =        0x1000,
        GatherLibraryFunc =     0x2000,

        GatherLoadFunc =        0x4000,

        [Description("CreateFile2")]
        GatherCreateFile2 =     GatherFileFunc | 0x0001,
        [Description("CreateFileA")]
        GatherCreateFileA =     GatherFileFunc | 0x0002,
        [Description("CreateFileW")]
        GatherCreateFileW =     GatherFileFunc | 0x0004,
        [Description("CreateFileById")]
        GatherOpenFileById =    GatherFileFunc | 0x0008,

        [Description("LoadLibraryA")]
        GatherLoadLibraryA =    GatherLibraryFunc | 0x0001,
        [Description("LoadLibraryW")]
        GatherLoadLibraryW =    GatherLibraryFunc | 0x0002,
        [Description("LoadLibraryExA")]
        GatherLoadLibraryExA =  GatherLibraryFunc | 0x0004,
        [Description("LoadLibraryExW")]
        GatherLoadLibraryExW =  GatherLibraryFunc | 0x0008,

        [Description("GatherConnection")]
        GatherConnection =      GatherLoadFunc | 0x0001,
        [Description("FilesOnLoad")]
        GatherFilesOnLoad =     GatherLoadFunc | 0x0002
    }

    [Flags]
    public enum GatherType : UInt16
    {
        GatherNone =            0x0000,

        GatherStatus =          0x1000,
        GatherResource =        0x2000,
        GatherWarning =         0x4000,

        [Description("Gathering has been activated")]
        GatherActivated =       GatherStatus | 0x0001,
        [Description("Gathering has been stopped")]
        GatherDeactivated =     GatherStatus | 0x0002,
        [Description("Gathering is up")]
        GatherStillUp =         GatherStatus | 0x0004,

        [Description("File")]
        GatherFile =            GatherResource | 0x0001,

        [Description("Library")]
        GatherLibrary =         GatherResource | 0x0002
    }
}