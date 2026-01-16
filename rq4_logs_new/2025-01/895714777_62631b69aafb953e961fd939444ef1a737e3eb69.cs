using Godot;

public partial class MenuButtonOpen : Button
{
	// Phương thức _Ready được gọi khi button được khởi tạo
	public override void _Ready()
	{
		// Kết nối tín hiệu "pressed" của Button đến phương thức OnButtonPressed
		this.Pressed += OnButtonPressed;
	}

	// Phương thức sẽ được gọi khi button được nhấn
	private void OnButtonPressed()
	{
		CanvasLayer menu = GetNode<CanvasLayer>("../Menu");
		menu.GetNode<Label>("./GameOverLabel").Visible = false;
		menu.GetNode<Button>("./Button/Continue").Disabled = false;
		menu.GetNode<Button>("./Button/Setting").Disabled = false;
		menu.Visible = true;
		GD.Print("Menu displayed!");
	}
}