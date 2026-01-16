using Godot;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace MafiaHostAssistant;

public partial class BehaviorSelectionPanel : Control
{
	[Export] private PackedScene itemScene;
	
	[Export] private Label label;
	[Export] private Dropdown dropdown;
	[Export] private Label configsGroupLabel;
	[Export] private Control configsContent;
	[Export] private Label accessedVarsGroupLabel;
	[Export] private Control accessedVariablesContent;
	[Export] private Label createdVarsGroupLabel;
	[Export] private Control createdVariablesContent;
	
	private RoleEditor roleEditor;
	private readonly List<string> paths = new();
	
	private BehaviorRecord currentRecord;
	private int currentRecordIndex;
	private readonly Dictionary<string, object> config = new();
	
	public void SetUp(string label, BehaviorType behaviorType, RoleEditor roleEditor)
	{
		this.label.Text = label;
		this.roleEditor = roleEditor;
		
		List<Dropdown.ElementData> elements = new();
		
		int doNothingIndex = -1;

        string directory = behaviorType switch
        {
            BehaviorType.ActiveActionWithPlayer => FilePaths.GetActiveActionsWithPlayersDirectoryPath(),
            BehaviorType.PassiveActionWithPlayer => FilePaths.GetPassiveActionsWithPlayersDirectoryPath(),
            BehaviorType.WakingAlgorythm => FilePaths.GetWakingAlgorythmsDirectoryPath(),
            BehaviorType.PassiveActionWithUnion => FilePaths.GetPassiveActionsWithUnionsDirectoryPath(),
            _ => string.Empty,
        };

		string defaultActionName = behaviorType switch
        {
            BehaviorType.ActiveActionWithPlayer => "@DoNothing",
            BehaviorType.PassiveActionWithPlayer => "@DoNothing",
            BehaviorType.WakingAlgorythm => "@AlwaysWake",
            BehaviorType.PassiveActionWithUnion => "@DoNothing",
			_ => string.Empty
        };

        foreach (string path in Directory.EnumerateFiles(directory))
		{
			string name = Path.GetFileNameWithoutExtension(path);
			elements.Add(new Dropdown.ElementData(null, Translator.TryTranslateBehaviorName(name)));
			paths.Add(path);
			if (name == defaultActionName)
			{
				doNothingIndex = paths.Count - 1;
			}
		}
		dropdown.AddElements(elements); // ItemSelected is bound in Editor
		dropdown.Current = doNothingIndex;
    }

    public void OnBehaviorSelected(int index)
	{
		config.Clear();
		foreach (Node field in configsContent.GetChildren())
		{
			field.QueueFree();
		}
		
		BehaviorRecord record = JsonConvert.DeserializeObject<BehaviorRecord>(File.ReadAllText(paths[index]));
		currentRecord = record;
		currentRecordIndex = index;
		
		if (record.ConfigurableVariables.Length == 0)
		{
			configsGroupLabel.Visible = false;
			configsContent.Visible = false;
		}
		else
		{
			foreach (VariableRecord confVar in record.ConfigurableVariables)
			{
				config.Add(confVar.varName, null);
				switch (confVar.varType)
				{
					case BehaviorVariableType.Bool:
						{
							NamedBoolConfigField field = Cache.Instance.NamedBoolFieldScene.Instantiate<NamedBoolConfigField>();
							field.SetUp(confVar.varName, false, RegisterConfig);
							configsContent.AddChild(field);
						}
						break;

					case BehaviorVariableType.Integer:
						{
							NamedIntConfigField field = Cache.Instance.NamedIntFieldScene.Instantiate<NamedIntConfigField>();
							field.SetUp(confVar.varName, 0, true, RegisterConfig);
							configsContent.AddChild(field);
						}
						break;

					case BehaviorVariableType.String:
						{
							NamedStringConfigField field = Cache.Instance.NamedStringFieldScene.Instantiate<NamedStringConfigField>();
							field.SetUp(confVar.varName, string.Empty, StringFieldContext.Unrestricted, true, RegisterConfig);
							configsContent.AddChild(field);
						}
						break;

					case BehaviorVariableType.ListOfBools:
						{
							NamedListBoolConfigField field = Cache.Instance.NamedListBoolField.Instantiate<NamedListBoolConfigField>();
							List<bool> list = new();
							RegisterConfig(list);
							field.SetUp(confVar.varName, list, Empty);
							configsContent.AddChild(field);
						}
						break;

					case BehaviorVariableType.ListOfInts:
						{
							NamedListIntConfigField field = Cache.Instance.NamedListIntField.Instantiate<NamedListIntConfigField>();
							List<int> list = new();
							RegisterConfig(list);
							field.SetUp(confVar.varName, list, Empty);
							configsContent.AddChild(field);
						}
						break;

					case BehaviorVariableType.ListOfStrings:
						{
							NamedListStringConfigField field = Cache.Instance.NamedListStringField.Instantiate<NamedListStringConfigField>();
							List<string> list = new();
							RegisterConfig(list);
							field.SetUp(confVar.varName, list, Empty);
							configsContent.AddChild(field);
						}
						break;

					default:
						break;
				}

				void RegisterConfig<T>(T v)
				{
					config[confVar.varName] = v;
				}

				void Empty() { }
			}
		}

		if (record.AccessedSharedVariables.Length == 0)
		{
			accessedVarsGroupLabel.Visible = false;
			accessedVariablesContent.Visible = false;
		}
		else
		{
			foreach (VariableRecord acVar in record.AccessedSharedVariables)
			{
				Item item = itemScene.Instantiate<Item>();
				item.icon.Texture = Cache.Instance.GetVariableTypeTexture(acVar.varType);
				item.label.Text = acVar.varName;
				accessedVariablesContent.AddChild(item);
			}
		}
		
		if (record.CreatedSharedVariables.Length == 0)
		{
			createdVarsGroupLabel.Visible = false;
			createdVariablesContent.Visible = false;
		}
		else
		{
			foreach (VariableRecord crVar in record.CreatedSharedVariables)
			{
				Item item = itemScene.Instantiate<Item>();
				item.icon.Texture = Cache.Instance.GetVariableTypeTexture(crVar.varType);
				item.label.Text = crVar.varName;
				createdVariablesContent.AddChild(item);
			}
			
			roleEditor?.AddCreatedVariables(record.CreatedSharedVariables);
		}

		// TODO: Set groups and labels invisible if no elements are there
	}
	
	// Can only be called if roleEditor is not null
	public void OnCreatedVariablesChanged()
	{
        for (int i = 0; i < currentRecord.AccessedSharedVariables.Length; i++)
		{
            VariableRecord acVar = currentRecord.AccessedSharedVariables[i];
			Label itemLabel = accessedVariablesContent.GetChild<Item>(i).label;
			if (roleEditor.createdVariables.Find(v => v.Equals(acVar)) == null)
			{
                if (!itemLabel.HasThemeColorOverride("font_color"))
				{
					itemLabel.AddThemeColorOverride("font_color", Cache.Instance.ErroringRed);
				}
			}
			else
			{
				if (itemLabel.HasThemeColorOverride("font_color"))
				{
					itemLabel.RemoveThemeColorOverride("font_color");
				}
			}
        }
	}
	
	public bool IsValid()
	{
	    foreach (VariableRecord acVar in currentRecord.AccessedSharedVariables)
		{
			if (roleEditor.createdVariables.Find(v => v.Equals(acVar)) == null)
			{
				return false;
			}
		}
		return true;
	}
	
	public ConfiguredBehaviorRecord Read()
	{
	    return new ConfiguredBehaviorRecord(Path.GetFileNameWithoutExtension(paths[currentRecordIndex]), config);
	}
    // TODO: Figure out a "Write" method to allow editing roles that have already been created.

}