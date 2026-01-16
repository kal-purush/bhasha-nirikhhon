using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Build.Framework;
using Newtonsoft.Json;
using MSBuildTask = Microsoft.Build.Utilities.Task;

namespace Parithon.StreamDeck.SDK.MSBuild
{
  public class CreateDistribution : MSBuildTask
  {
    [Output]
    public bool DistributionReady { get; set; }

    [Output]
    public string NotReadyReason { get; set; } = String.Empty;

    [Required]
    public string PublishDir { get; set; }

    public override bool Execute()
    {
      var manifestPath = Path.Combine(PublishDir, "manifest.json");
      if (!File.Exists(manifestPath)) return true;
      var manifestJSON = File.ReadAllText(manifestPath);
      var manifest = JsonConvert.DeserializeObject<Manifest>(manifestJSON);
      var codepath = !string.IsNullOrEmpty(manifest.CodePath) && File.Exists(Path.Combine(PublishDir, manifest.CodePath));
      var codepathmac = string.IsNullOrEmpty(manifest.CodePathMac) || File.Exists(Path.Combine(PublishDir, manifest.CodePathMac));
      var codepathwin = string.IsNullOrEmpty(manifest.CodePathWin) || File.Exists(Path.Combine(PublishDir, manifest.CodePathWin));
      DistributionReady = (codepath && codepathmac && codepathwin);
      if (!DistributionReady)
      {
        NotReadyReason = "Publish did not generate a streamDeckPlugin distribution because:";
      }
      if (!codepath)
      {
        NotReadyReason += $"\n\tCould not find the {manifest.CodePath} executable.";
      }
      if (!codepathmac)
      {
        NotReadyReason += $"\n\tCould not find the {manifest.CodePathMac} executable.";
      }
      if (!codepathwin)
      {
        NotReadyReason += $"\n\tCould not find the {manifest.CodePathWin} executable.";
      }
      if (!DistributionReady)
      {
        NotReadyReason += $"\nPublish the additional executables and the streamDeckPlugin distribution package will be generated automatically.";
      }
      return true;
    }
  }
}