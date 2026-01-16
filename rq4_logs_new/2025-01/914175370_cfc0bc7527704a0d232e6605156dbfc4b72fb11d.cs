using NLog;
using PenumbraModForwarder.Common.Interfaces;
using SevenZipExtractor;

namespace PenumbraModForwarder.Common.Services;

public class DownloadUpdater : IDownloadUpdater
{
    private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
    private readonly IUpdateService _updateService;
    private readonly IAria2Service _aria2Service;

    public DownloadUpdater(IUpdateService updateService, IAria2Service aria2Service)
    {
        _updateService = updateService;
        _aria2Service = aria2Service;
    }

    public async Task<string?> DownloadAndExtractLatestUpdaterAsync(string outputFolder, CancellationToken ct)
    {
        _logger.Debug("Entered `DownloadAndExtractLatestUpdaterAsync` with `outputFolder`: {OutputFolder}", outputFolder);
        
        var latestRelease = await _updateService.GetLatestReleaseAsync(false, "CouncilOfTsukuyomi/Updater");
        if (latestRelease == null)
        {
            _logger.Warn("No releases returned. Aborting updater download.");
            return null;
        }

        if (latestRelease.Assets == null || latestRelease.Assets.Count == 0)
        {
            _logger.Warn("Release found, but no assets available for download. Aborting.");
            return null;
        }
        
        var zipAsset = latestRelease.Assets
            .FirstOrDefault(a => a.Name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase));
        if (zipAsset == null)
        {
            _logger.Warn("No .zip asset found in the release. Aborting updater download.");
            return null;
        }

        _logger.Info("Found updater asset: {Name}, Download URL: {Url}", zipAsset.Name, zipAsset.BrowserDownloadUrl);
        
        var tempFolder = Path.Combine(Path.GetTempPath(), "CouncilOfTsukuyomi");
        if (!Directory.Exists(tempFolder))
        {
            Directory.CreateDirectory(tempFolder);
            _logger.Debug("Created temp folder at `{TempFolder}`.", tempFolder);
        }
        
        _logger.Debug("Starting download of updater asset...");
        var downloadSucceeded = await _aria2Service.DownloadFileAsync(zipAsset.BrowserDownloadUrl, tempFolder, ct);
        if (!downloadSucceeded)
        {
            _logger.Error("Download failed for updater asset: {AssetName}", zipAsset.Name);
            return null;
        }
        _logger.Info("Updater asset download complete.");
        
        var downloadedZipPath = Path.Combine(tempFolder, Path.GetFileName(new Uri(zipAsset.BrowserDownloadUrl).AbsolutePath));
        _logger.Debug("Local path to downloaded zip: {DownloadedZipPath}", downloadedZipPath);
        
        if (!Directory.Exists(outputFolder))
        {
            Directory.CreateDirectory(outputFolder);
            _logger.Debug("Created or verified existence of the output folder `{OutputFolder}`.", outputFolder);
        }

        try
        {
            _logger.Debug("Beginning extraction of the downloaded zip.");
            using (var archive = new ArchiveFile(downloadedZipPath))
            {
                archive.Extract(outputFolder, overwrite: true);
            }
            _logger.Info("Extraction completed successfully to `{OutputFolder}`.", outputFolder);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error encountered while extracting updater archive.");
            throw;
        }
        finally
        {
            if (File.Exists(downloadedZipPath))
            {
                try
                {
                    File.Delete(downloadedZipPath);
                    _logger.Debug("Removed temporary zip file `{DownloadedZipPath}`.", downloadedZipPath);
                }
                catch (Exception cleanupEx)
                {
                    _logger.Warn(cleanupEx, "Failed to clean up the .zip file at `{DownloadedZipPath}`.", downloadedZipPath);
                }
            }
        }
        
        var updaterPath = Path.Combine(outputFolder, "Updater.exe");
        if (File.Exists(updaterPath))
        {
            _logger.Debug("Updater.exe found at `{UpdaterPath}`. Returning path.", updaterPath);
            return updaterPath;
        }

        _logger.Warn("Updater.exe not found in `{OutputFolder}` after extraction.", outputFolder);
        return null;
    }
}