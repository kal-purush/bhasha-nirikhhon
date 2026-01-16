using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Windows.Input;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using DesktopAssistant.Core.Models;
using Windows.ApplicationModel.Chat;

namespace DesktopAssistant.ViewModels.Popup;

public partial class ChatViewModel : ObservableRecipient
{
    [ObservableProperty]
    private string userMessage = string.Empty;

    public ObservableCollection<ChatMessage> ChatMessages { get; } = [];

    [RelayCommand]
    public void SendMessage()
    {
        // ユーザメッセージをリストに追加  
        ChatMessages.Add(new ChatMessage { UserName = "User", Message = UserMessage, HorizontalAlignment = "Right", BackgroundColor = "#ADD8E6" });

        // AIのレスポンスを生成する処理をここに追加  
        // 簡単のため、ここではユーザのメッセージをエコーバックするようにします  
        ChatMessages.Add(new ChatMessage { UserName = "AI", Message = UserMessage, HorizontalAlignment = "Left", BackgroundColor = "#FFA07A" });

        // メッセージボックスをクリア  
        UserMessage = string.Empty;
    }

    [RelayCommand]
    public void Retry()
    {
        // メッセージリストをクリア  
        ChatMessages.Clear();
    }
}

public class ChatMessage
{
    public string? UserName
    {
        get; set;
    }
    public string? Message
    {
        get; set;
    }
    public string? HorizontalAlignment
    {
        get; set;
    }
    public string? BackgroundColor
    {
        get; set;
    }
}