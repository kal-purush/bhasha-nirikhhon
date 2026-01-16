using System;
using System.Collections.Generic;
using System.Text;
using Terminal.Gui;

namespace ConsoleChat
{
    class ChatForm
    {
		public void Initialize()
		{
			Application.Init();
			var top = Application.Top;

			var win_Dialogs = new Window("NameDialogs")
			{
				X = 0,
				Y = 0,

				Width = 50,
				Height = 17
			};
			var win_InptMessage = new Window("Message")
			{
				X = 0,
				Y = Pos.Bottom(win_Dialogs),

				Width = 50,
				Height = Dim.Fill()
			};
			List<string> list_message = new List<string>();
			var list_Message = new ListView(list_message)
			{
				X = 0,
				Y = 0,
				Height = Dim.Fill(),
				Width = Dim.Fill()
			};
			var win_ShowDialogs = new Window("Dialogs")
			{
				X = Pos.Right(win_Dialogs),
				Y = 0,

				Width = Dim.Fill(),
				Height = Dim.Fill()
			};

			var MessageText = new TextField()
			{
				X = 0,
				Y = 0,

				Width = 38,
				Height = Dim.Fill(),

			};
			var but_Enter = new Button("Enter")
			{
				X = Pos.Right(MessageText),
				Y = 0,
				Width = 10,
				Height = 1
			};
			List<string> list_dialogs = new List<string>();
			var list_Dialogs = new ListView()
			{
				X = 1,
				Y = 1,
				Width = Dim.Fill(),
				Height = Dim.Fill(),
			};

			but_Enter.Clicked += () => {
				list_message.Add($"me: {MessageText.Text.ToString()}");
				MessageText.Text = "";
			};

			top.Add(win_Dialogs);
			top.Add(win_InptMessage);
			top.Add(win_ShowDialogs);

			win_Dialogs.Add(
				list_Message);

			win_InptMessage.Add(
				MessageText, but_Enter);

			win_ShowDialogs.Add(
				list_Dialogs);


			Application.Run();
		}
	}
}