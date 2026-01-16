using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EasySave.Model;

public class IBackupJobEventArgs : EventArgs {
    public string? JobName { get; }
}
public class IBackupJobProgressEventArgs : IBackupJobEventArgs {
    public int? Progress { get; }
}
public class IBackupJobErrorEventArgs : IBackupJobEventArgs {
    public string? ErrorMessage { get; }
}
public class IBackupJobCancelledEventArgs : IBackupJobEventArgs {
    public string? CancelMessage { get; }
}
public delegate void BackupJobEventHandler(object sender, IBackupJobEventArgs e);
public delegate void BackupJobProgressEventHandler(object sender, IBackupJobProgressEventArgs e);
public delegate void BackupJobErrorEventHandler(object sender, IBackupJobErrorEventArgs e);
public delegate void BackupJobCancelledEventHandler(object sender, IBackupJobCancelledEventArgs e);

public interface IBackupJob {
    public string Name { get; }
    public IDirectory Source { get; }
    public IDirectory Destination { get; }
    public List<IBackupTask> Tasks { get; }
    public int CurrentTask { get; }

    public abstract void Analyse();
    public abstract void Run();

    public event BackupJobEventHandler? BackupJobStarted;
    public event BackupJobProgressEventHandler? BackupJobProgress;
    public event BackupJobEventHandler? BackupJobPaused;
    public event BackupJobEventHandler? BackupJobResumed;
    public event BackupJobEventHandler? BackupJobFinished;
    public event BackupJobErrorEventHandler? BackupJobError;
    public event BackupJobCancelledEventHandler? BackupJobCancelled;
}

internal class BackupJob : IBackupJob { 
}
