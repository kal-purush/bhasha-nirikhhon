using System;
using System.Windows.Controls;
using System.Collections.Generic;
using System.Windows;
using MahApps.Metro.Controls;

namespace ImageManager
{
	class ControlPanel
	{
		const string DELETE_KEY_BUTTON_NAME = "DeleteKeyButton{0}";
		const string KEY_TEXTBOX_NAME = "KeyTextBox{0}";
		const string DIRECTORY_NAME_TEXTBOX_NAME = "SubfolderTextBox{0}";
		const string CONTROL_ROW_NAME = "ControlRow{0}";
		const string ADD_KEY_BUTTON_NAME = "AddKeyButton";

		public int Index { get; private set; }
		public Button DeleteKeyButton { get; }
		public TextBox KeyTextBox { get; }
		public TextBox SubfolderTextBox { get; }
		public RowDefinition ControlRow { get; }
		public StackPanel ControlStackPanel { get; }
		public Grid ControlGrid { get; }

		private enum TextBoxType
		{
			KeyTextBox, SubfolderTextBox
		}

		private enum ButtonType
		{
			AddKey, DeleteKey
		}

		public ControlPanel(int index)
		{
			Index = index;
			DeleteKeyButton = CreateButton(ButtonType.DeleteKey);
			KeyTextBox = CreateTextBox(TextBoxType.KeyTextBox);
			SubfolderTextBox = CreateTextBox(TextBoxType.SubfolderTextBox);

			ControlRow = CreateRowDefinition();
			ControlGrid = CreateGrid();
			FillGrid();
			ControlStackPanel = CreateStackPanel();
			FillStackPanel();
		}

		public static int GetIndex(IFrameworkInputElement element)
		{
			int index = Convert.ToInt32(element.Name.Remove(0, element.Name.Length - 1));
			return index;
		}

		public static ControlPanel GetControlPanel(List<ControlPanel> controlPanels, int index)
		{
			foreach (ControlPanel cp in controlPanels)
			{
				if (cp.Index == index)
					return cp;
			}

			return null;
		}

		public static StackPanel CreateHeaderStackPanel(RoutedEventHandler dlg)
		{
			ControlPanel cp = new ControlPanel(0);
			var stackPanel = new StackPanel();
			Grid.SetRow(stackPanel, 0);
			var button = cp.CreateButton(ButtonType.AddKey);
			button.Click += dlg;
			var fileModeSwitcher = cp.CreateToggleSwitcher();
			var grid = new Grid();

			grid.Children.Add(button);
			grid.Children.Add(fileModeSwitcher);
			stackPanel.Children.Add(grid);

			return stackPanel;
		}

		private ToggleSwitch CreateToggleSwitcher()
		{
			var fileModeSwitcher = new ToggleSwitch();

			fileModeSwitcher.Style = fileModeSwitcher.FindResource("FileModeSwitchStyle") as Style;
			fileModeSwitcher.Name = "FileModeSwitcher";

			return fileModeSwitcher;
		}


		public static void ChangeIndex(List<ControlPanel> controlPanels)
		{
			var index = 1;
			foreach (ControlPanel cp in controlPanels)
			{
				cp.Index = index;
				cp.KeyTextBox.Name = String.Format(KEY_TEXTBOX_NAME, index);
				cp.SubfolderTextBox.Name = String.Format(DIRECTORY_NAME_TEXTBOX_NAME, index);
				cp.DeleteKeyButton.Name = String.Format(DELETE_KEY_BUTTON_NAME, index);
				cp.ControlRow.Name = String.Format(CONTROL_ROW_NAME, index);

				Grid.SetRow(cp.DeleteKeyButton, cp.Index);
				Grid.SetRow(cp.KeyTextBox, cp.Index);
				Grid.SetRow(cp.SubfolderTextBox, cp.Index);
				Grid.SetRow(cp.ControlStackPanel, cp.Index);

				index++;
			}
		}

		private void FillStackPanel()
		{
			ControlStackPanel.Children.Add(ControlGrid);
		}

		public static string GetSubfolderName(List<ControlPanel> controlPanels, int index)
		{
			foreach (ControlPanel cp in controlPanels)
			{
				if (cp.Index == index)
				{
					return cp.SubfolderTextBox.Text;
				}
			}
			return String.Empty;
		}

		public static int GetControlPanelIndex(List<ControlPanel> controlPanels, string key)
		{
			foreach (ControlPanel cp in controlPanels)
			{
				if (cp.KeyTextBox.Text == key)
				{
					return cp.Index;
				}
			}

			return 0;
		}

		private void FillGrid()
		{
			ControlGrid.Children.Add(KeyTextBox);
			ControlGrid.Children.Add(SubfolderTextBox);
			ControlGrid.Children.Add(DeleteKeyButton);
		}

		private Button CreateButton(ButtonType type)
		{
			var button = new Button
			{
				Name = type == ButtonType.DeleteKey ?
					String.Format(DELETE_KEY_BUTTON_NAME, Index) :
					ADD_KEY_BUTTON_NAME
			};

			button.Style = type == ButtonType.DeleteKey ?
				button.FindResource("DeleteKeyButtonStyle") as Style :
				button.FindResource("AddKeyButtonStyle") as Style;

			return button;
		}

		private TextBox CreateTextBox(TextBoxType type)
		{
			TextBox textBox = new TextBox
			{
				Name = type == TextBoxType.SubfolderTextBox ?
					String.Format(DIRECTORY_NAME_TEXTBOX_NAME, Index) :
					String.Format(KEY_TEXTBOX_NAME, Index)

			};

			textBox.Style = type == TextBoxType.SubfolderTextBox ?
					textBox.FindResource("SubfolderNameTextBoxStyle") as Style :
					textBox.FindResource("KeyTextBoxStyle") as Style;

			return textBox;
		}

		private RowDefinition CreateRowDefinition()
		{
			RowDefinition rd = new RowDefinition();
			rd.Height = new GridLength(55, GridUnitType.Pixel);
			rd.Name = String.Format(CONTROL_ROW_NAME, Index);
			return rd;
		}

		private StackPanel CreateStackPanel()
		{
			StackPanel stackPanel = new StackPanel();

			Grid.SetRow(stackPanel, Index);

			return stackPanel;
		}

		private Grid CreateGrid()
		{
			Grid grid = new Grid();

			return grid;
		}

		public void SetKey(string key)
		{
			KeyTextBox.Text = key;
		}

		public void SetDirectory(string directoryName)
		{
			SubfolderTextBox.Text = directoryName;
		}
	}
}