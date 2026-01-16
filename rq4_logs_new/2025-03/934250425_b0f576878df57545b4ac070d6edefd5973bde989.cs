using Godot;
using System;
using System.Xml.Serialization;

public partial class Ui : CanvasLayer
{

	[Signal]
	public delegate void IncreseDifficultyEventHandler();

	[Export]
	private int _scoreToIncreaseDifficulty = 50;	

	[Export]
	private float _pointupdateInterval = 0.3f;

	private float _pointUpdateTimer = 0.0f;

	private Label _scoreLabel;

	private int _score;

	private Player _player;

	private TextureButton _restartButton;

	private VBoxContainer _gameOverContainer;

	// Called when the node enters the scene tree for the first time.
	public override void _Ready()
	{
		_gameOverContainer = GetNode<VBoxContainer>("%GameOverContainer");
		_restartButton = GetNode<TextureButton>("%Restart");
		_player = GetNode<Player>("/root/Main/Player");

		_scoreLabel = GetNode<Label>("%ScoreLabel");
		_score = 0;
		_scoreLabel.Text = $"{_score.ToString("D5")}";

		_player.ObstacleHit += () => {
			_gameOverContainer.Visible = true;
			SetProcess(false);
		};

		_restartButton.Pressed += () => GetTree().ReloadCurrentScene();
	}

	// Called every frame. 'delta' is the elapsed time since the previous frame.
	public override void _Process(double delta)
	{
		_pointUpdateTimer += (float)delta;
		if (_pointUpdateTimer >= _pointupdateInterval) {
			_score++;
			_scoreLabel.Text = $"{_score.ToString("D5")}";
			_pointUpdateTimer = 0.0f;

			if (_score % _scoreToIncreaseDifficulty == 0){
			EmitSignal(SignalName.IncreseDifficulty);
			}
		}

	}
}