using System.Management;
using System.Runtime.InteropServices;
using PenumbraModForwarder.Common.Enums;

namespace PenumbraModForwarder.Common.Extensions;

public static class DriveTypeDetector
{
    public static DriveTypeCommon GetDriveType(string path)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return GetDriveTypeWindows(path);
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return GetDriveTypeLinux(path);
        }
        else
        {
            return DriveTypeCommon.None;
        }
    }
    private static DriveTypeCommon GetDriveTypeWindows(string path)
    {
        try
        {
            var rootPath = Path.GetPathRoot(path);

            if (string.IsNullOrEmpty(rootPath))
            {
                return DriveTypeCommon.None;
            }

            rootPath = rootPath[0].ToString();

            var scope = new ManagementScope(@"\\.\root\microsoft\windows\storage");
            scope.Connect();

            using var partitionSearcher = new ManagementObjectSearcher($"SELECT * FROM MSFT_Partition WHERE DriveLetter='{rootPath}'");
            partitionSearcher.Scope = scope;

            var partitions = partitionSearcher.Get();

            if (partitions.Count == 0)
            {
                return DriveTypeCommon.None;
            }

            string? diskNumber = null;

            foreach (var currentPartition in partitions)
            {
                diskNumber = currentPartition["DiskNumber"]?.ToString();

                if (!string.IsNullOrEmpty(diskNumber))
                {
                    break;
                }
            }

            if (string.IsNullOrEmpty(diskNumber))
            {
                return DriveTypeCommon.None;
            }

            using var diskSearcher = new ManagementObjectSearcher($"SELECT * FROM MSFT_PhysicalDisk WHERE DeviceId='{diskNumber}'");
            diskSearcher.Scope = scope;

            var physicalDisks = diskSearcher.Get();

            if (physicalDisks.Count == 0)
            {
                return DriveTypeCommon.None;
            }

            foreach (var currentDisk in physicalDisks)
            {
                var mediaType = Convert.ToInt16(currentDisk["MediaType"]);

                return mediaType switch
                {
                    3 => DriveTypeCommon.Hdd,
                    4 => DriveTypeCommon.Ssd,
                    _ => DriveTypeCommon.None,
                };
            }

            return DriveTypeCommon.None;
        }
        catch
        {
            return DriveTypeCommon.None;
        }
    }

    private static DriveTypeCommon GetDriveTypeLinux(string path)
    {
        try
        {
            var drive = Path.GetPathRoot(path)?.TrimEnd('/');
            if (string.IsNullOrEmpty(drive))
                return DriveTypeCommon.None;

            var blockDevice = Path.GetFileName(drive);

            var rotationalPath = $"/sys/block/{blockDevice}/queue/rotational";
            if (!File.Exists(rotationalPath))
                return DriveTypeCommon.None;

            var rotational = File.ReadAllText(rotationalPath).Trim();
            return rotational == "0" ? DriveTypeCommon.Ssd : DriveTypeCommon.Hdd;
        }
        catch
        {
            return DriveTypeCommon.None;
        }
    }
}