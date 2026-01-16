using Legion.Models.Internal;
using SkiaSharp;
using Splat;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Legion.Helpers
{
    internal class PathHelper
    {
        public static string generatePath(Models.Contract contract)
        {
            string subPath = string.Empty;
            if (contract.ContractType.TypeName.Contains("ТАНАКА"))
            {
                subPath = $"{Settings.ArchievFolder}" +
                $"/Договор МКК {contract.CustomId.Replace("/", ".")} " +
                $"{contract.Investor.LastName} {contract.Investor.FirstName[0]}. {contract.Investor.MiddleName[0]}";
            }
            else if (contract.ContractType.TypeName.Contains("Накопительный"))
            {
                subPath = $"{Settings.ArchievFolder}" +
                $"/Договор Накопительный {contract.CustomId.Replace("/", ".")} " +
                $"{contract.Investor.LastName} {contract.Investor.FirstName[0]}. {contract.Investor.MiddleName[0]}";
            }
            else
            {
                subPath = $"{Settings.ArchievFolder}" +
                $"/Договор {contract.CustomId.Replace("/", ".")} " +
                $"{contract.Investor.LastName} {contract.Investor.FirstName[0]}. {contract.Investor.MiddleName[0]}";
            }

            bool exists = Directory.Exists(subPath);

            if (!exists)
                Directory.CreateDirectory(subPath);

            return subPath;
        }

        public static Settings Settings => Locator.Current.GetService<Settings>()!;
    }
}