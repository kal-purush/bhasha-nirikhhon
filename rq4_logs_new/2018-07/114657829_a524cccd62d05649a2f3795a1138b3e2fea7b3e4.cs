// keyboard_layout.cs
// Original code by David Anderle 'davidanderle'
// Usage: Put the compiled keyboard_layout.exe to System32 folder, and call it
// from WSL as 'keyboard_layout.exe'.

using System;
using System.Windows.Forms;
using System.Threading;

public class KeyboardLayout{
	[STAThread]
	static void Main( string[] args )
	{
        string language = InputLanguage.CurrentInputLanguage.Culture.Name;
        try {
            language = language.Split('-')[1];
        }
        catch(Exception e) {
            return;
        }

        if(language != "GB")
            Console.Write("#(tmux set status-bg colour9)"+language+"\n");
        else
            Console.Write("#(tmux set status-bg colour235)"+language+"\n");
	}
}