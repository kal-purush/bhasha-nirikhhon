using Attack.Game;
using Godot;
using System;

public partial class Notification : Control
{
	private Label _label;
	private Button _button;
	private PanelContainer _container;

    private GameMaster _gameMaster;

	// Called when the node enters the scene tree for the first time.
	public override void _Ready()
	{
		_container = GetNode<PanelContainer>("PanelContainer");

        var path = "PanelContainer/MarginContainer/VBoxContainer";

		_label = GetNode<Label>(path + "/NotificationLabel");
        _button = GetNode<Button>(path + "/AcceptButton");

		_gameMaster = GetNode<GameMaster>("/root/GameMaster");

		_gameMaster.RegisterNotification(this);

        Visible = false;
    }

	public void SendNotification(string label, string button = "OK")
	{
		_label.Text = label;
		_button.Text = button;

		Visible = true;

		var x = (_container.Size.X / 2) * -1;
        var y = (_container.Size.Y / 2) * -1;

        // adjust anchor based on size
        Position = GetViewportRect().Size / 2 + new Vector2(x, y);
    }

	public void AcceptButtonPressed()
	{
        Visible = false;
		// TODO call back to master
	}
}