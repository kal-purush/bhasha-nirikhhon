using Godot;
using System;
using System.Collections.Generic;

public partial class Shop : CanvasLayer
{
    private VBoxContainer baseDamageItem;
    private VBoxContainer healthItem;

    [Export] public int BaseDamageCost = 2;
    [Export] public int HealthCost = 10;
    [Export] public int SkillCost = 20;

    private GridContainer itemGrid;
    private readonly Dictionary<string, VBoxContainer> skillItems = new();
    private Label goldLabel;

    // Called when the node enters the scene tree for the first time.
    public override void _Ready()
    {
        itemGrid = GetNode<GridContainer>("ItemGrid");
        goldLabel = GetNode<Label>("/root/world/TileMap/Player/GoldLabel");
        if (goldLabel == null)
        {
            GD.PrintErr("goldLabel not found!");
            return;
        }
        UpdateGoldLabel();

        if (itemGrid == null)
        {
            GD.PrintErr("ItemGrid not found!");
            return;
        }

        baseDamageItem = itemGrid.GetNode<VBoxContainer>("BaseDamageItem");
        SetupBaseDamageItem();

        healthItem = itemGrid.GetNode<VBoxContainer>("HealthItem");
        SetupHealthItem();

        var skillMappings = new Dictionary<string, string>
        {
            { "Dash", "Skill1Item" },
			// Add more mappings here for additional skills
		};

        foreach (var skillMapping in skillMappings)
        {
            string skillName = skillMapping.Key;
            string skillItemNodeName = skillMapping.Value;

            if (itemGrid.HasNode(skillItemNodeName))
            {
                var skillItem = itemGrid.GetNode<VBoxContainer>(skillItemNodeName);
                skillItems[skillName] = skillItem;
                SetupSkillItem(skillItem, skillName);
            }
            else
            {
                GD.PrintErr($"Node {skillItemNodeName} not found for skill {skillName}.");
            }
        }

        UpdateShopUI();
    }
    private void UpdateGoldLabel()
    {
        if (goldLabel != null)
        {
            goldLabel.Text = $"Gold: {Global.Gold}";
        }
    }


    private void SetupBaseDamageItem()
    {
        // Thiết lập thông tin cho BaseDamageItem
        var info = baseDamageItem.GetNode<Label>("Info");
        info.Text = $"(+5) - {BaseDamageCost} Gold";

        var buyButton = baseDamageItem.GetNode<Button>("BuyButton");
        buyButton.Text = "Buy";

        buyButton.Connect("pressed", Callable.From(() =>
        {
            if (Global.Gold >= BaseDamageCost)
            {
                Global.Gold -= BaseDamageCost;
                Global.PlayerBaseDamage += 5;
                GD.Print($"Base Damage increased to {Global.PlayerBaseDamage}");
                UpdateGoldLabel();
                UpdateShopUI();
            }
            else
            {
                GD.Print("Not enough gold!");
            }
        }));
    }

    private void SetupHealthItem()
    {
        // Thiết lập thông tin cho HealthItem
        var info = healthItem.GetNode<Label>("Info");
        info.Text = $"(+5) - {HealthCost} Gold";

        var buyButton = healthItem.GetNode<Button>("BuyButton");
        buyButton.Text = "Buy";

        buyButton.Connect("pressed", Callable.From(() =>
        {
            if (Global.Gold >= HealthCost)
            {
                Global.Gold -= HealthCost;
                Global.PlayerInstance.maxHealth += 5;
                Global.PlayerInstance.currentHealth += 5;
                GD.Print($"Health increased to {Global.PlayerInstance.maxHealth}");
                UpdateGoldLabel();
                UpdateShopUI();
            }
            else
            {
                GD.Print("Not enough gold!");
            }
        }));
    }

    private void SetupSkillItem(VBoxContainer skillItem, string skillName)
    {
        if (!Global.Skills.ContainsKey(skillName))
        {
            GD.PrintErr($"Skill {skillName} not found in Global.Skills!");
            return;
        }

        var skill = Global.Skills[skillName];

        // Set up the skill info
        var info = skillItem.GetNode<Label>("Info");
        info.Text = $"{skill.Name} - {SkillCost} Gold";

        var buyButton = skillItem.GetNode<Button>("BuyButton");
        buyButton.Text = skill.IsActive ? "Owned" : "Buy";

        buyButton.Connect("pressed", Callable.From(() =>
        {
            if (Global.Gold >= SkillCost && !skill.IsActive)
            {
                Global.Gold -= SkillCost;
                skill.IsActive = true;
                GD.Print($"{skill.Name} purchased!");
                UpdateGoldLabel();
                UpdateShopUI();
            }
            else
            {
                GD.Print(skill.IsActive ? "Skill already owned!" : "Not enough gold!");
            }
        }));
    }

    private void UpdateShopUI()
    {
        // Cập nhật trạng thái UI của BaseDamageItem
        var baseDamageButton = baseDamageItem.GetNode<Button>("BuyButton");
        baseDamageButton.Text = $"Buy";

        // Cập nhật trạng thái UI của HealthItem
        var healthButton = healthItem.GetNode<Button>("BuyButton");
        healthButton.Text = $"Buy";

        // Cập nhật trạng thái UI của SkillItems
        foreach (var skillName in skillItems.Keys)
        {
            var skillItem = skillItems[skillName];
            var buyButton = skillItem.GetNode<Button>("BuyButton");
            var skill = Global.Skills[skillName];

            if (skill.IsActive)
            {
                buyButton.Text = "Owned";
                buyButton.Disabled = true;
            }
            else
            {
                buyButton.Text = $"Buy";
                buyButton.Disabled = false;
            }
        }
    }

    // Called every frame. 'delta' is the elapsed time since the previous frame.
    public override void _Process(double delta)
    {
    }
}