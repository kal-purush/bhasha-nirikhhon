using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Security.AccessControl;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace rydavidson.Accela.Utilities.Common
{
    public class AAUtil
    {
        public static Logger logger = new Logger("CommonUtils.log");
        static RegistryKey accelaBaseKey;
        static RegistryKey instanceKey;

        public static List<string> GetInstancesForVersion(string versionToCheck)
        {
            List<string> instances = new List<string>();
            try
            {
                RegistryKey accelaBaseKey = GetAccelaBaseKey();
                List<string> versions = GetAAVersions();
                foreach (string version in versions)
                {
                    if (version.Trim() == versionToCheck.Trim())
                    {
                        foreach (string instance in GetAAInstancesByVersion(version))
                        {
                            instances.Add(instance);
                        }
                    }
                }
            }
            catch (UnauthorizedAccessException uaex)
            {
                logger.LogError("UnauthorizedAccessException getting instances for version " + versionToCheck + ", error: " + uaex.Message + uaex.StackTrace);
            }
            catch (Exception e)
            {
                logger.LogError("Error getting instances for version " + versionToCheck + ", error: " + e.Message + e.StackTrace);
            }

            return instances;
        }

        private static RegistryKey GetAccelaBaseKey()
        {
            RegistryKey hklmReg;

            if (accelaBaseKey != null)
            {
                return accelaBaseKey;
            }
            try
            {
                hklmReg = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry64);
                try
                {
                    accelaBaseKey = hklmReg.OpenSubKey(@"SOFTWARE\WOW6432Node\Accela Inc.", RegistryKeyPermissionCheck.ReadSubTree);
                    return accelaBaseKey;
                }
                catch (Exception e)
                {
                    logger.LogError("Failed to open Accela key. Error: " + e.Message + " " + e.StackTrace);
                }
            }
            catch (Exception e)
            {
                logger.LogError("Failed to open HKLM. Error: " + e.Message + " " + e.StackTrace);
            }
            return accelaBaseKey;
        }

        private static RegistryKey GetInstanceKey(string _version, string _instance)
        {

            if (instanceKey != null)
                return instanceKey;

            RegistryKey reg = GetAccelaBaseKey();

            try
            {
                instanceKey = reg.OpenSubKey(string.Format(@"AA Base Installer\{0}\{1}", _version, _instance), RegistryKeyPermissionCheck.ReadSubTree);
            }
            catch (Exception ex)
            {
                logger.LogError("Error while reading install directory: " + ex.Message + ex.StackTrace);
            }

            return instanceKey;
        }

        public static List<string> GetAAVersions()
        {
            //if(GetAccelaBaseKey() == null)
            //{
            //    StringBuilder err = new StringBuilder();
            //    err.AppendLine("Couldn't open Accela Base Key");
            //    logger.LogError(err.ToString());
            //    return null;
            //}
            return new List<string>(GetAccelaBaseKey().OpenSubKey(@"AA Base Installer").GetSubKeyNames());
        }

        public static List<string> GetAAInstancesByVersion(string _version)
        {
            string[] instancekeys = GetAccelaBaseKey().OpenSubKey(@"AA Base Installer\" + _version).GetSubKeyNames();
            return new List<string>(instancekeys);
        }

        public static string GetAAInstallDir()
        {
            if(GlobalConfigs.Instance.AAInstallDir != null)
            {
                return GlobalConfigs.Instance.AAInstallDir;
            }
            string version = GlobalConfigs.Instance.AAVersion;
            string instance = GlobalConfigs.Instance.AAInstance;
            RegistryKey reg = GetAccelaBaseKey();
            string installDir = "";
            if (reg != null && version != null && instance != null)
            {
                try
                {
                    instanceKey = reg.OpenSubKey(string.Format(@"AA Base Installer\{0}\{1}", version, instance), RegistryKeyPermissionCheck.ReadSubTree);
                    installDir = instanceKey.GetValue("InstallDir").ToString();
                }
                catch (Exception ex)
                {
                    logger.LogError("Error while reading install directory: " + ex.Message + ex.StackTrace);
                }
            }
            if (installDir != "")
                GlobalConfigs.Instance.AAInstallDir = installDir;
            return installDir;
        }
        public static List<string> GetAAInstalledComponents(string _version, string _instance)
        {
            RegistryKey reg = GetAccelaBaseKey();
            string components = GetInstanceKey(_version, _instance).GetValue("InstallComponents").ToString();
            List<string> compList = new List<string>();
            foreach(string comp in components.Split(','))
            {
                compList.Add(comp);
            }
            return compList;
        }
        public static Dictionary<string,string> GetAAConfigFilePaths(string _version, string _instance)
        {
            Dictionary<string, string> paths = new Dictionary<string, string>();
            List<string> components = GetAAInstalledComponents(_version, _instance);
            string installDir = GetAAInstallDir();
            StringBuilder sb = new StringBuilder();

            foreach (string comp in components)
            {
                string stemp = string.Format(@"{0}\{1}\conf\av\ServerConfig.properties", installDir, comp);
                if (File.Exists(stemp))
                paths.Add(comp,stemp);
            }
            return paths;
        }

        public static String DecryptSecureString(SecureString value)
        {
            string val = new System.Net.NetworkCredential(string.Empty, value).Password; // not intended use but a convenient way to get the string from a SecureString without marshalling
            return val;
        }
    }
}