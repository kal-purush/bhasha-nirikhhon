using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Platform.Storage;

public interface IFileDialogService
{
    public Task<IStorageFile?> DoOpenFilePickerAsync();
    public Task<IStorageFile?> DoSaveFilePickerAsync();
}