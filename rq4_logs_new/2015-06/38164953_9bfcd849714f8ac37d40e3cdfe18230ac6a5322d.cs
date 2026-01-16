using UnityEngine;
using System.Collections;

public class MaterialWrapper{
	public Renderer renderer;
	public MaterialPropertyBlock matpropblock;

	public int colorID = 0;

	private Color _color;
	public Color color{
		get{
			return _color;
		}
		set{
			SetColor(value);
		}
	}



	public MaterialWrapper(Renderer renderer, string colorstring = "_Color"){
		this.renderer = renderer;
		this.matpropblock = new MaterialPropertyBlock();
		renderer.GetPropertyBlock(this.matpropblock);
		this.colorID = Shader.PropertyToID(colorstring);
	}

	public void SetColor(int ID, Color color){
		matpropblock.SetColor(ID, color);
		_color = color;
		Apply();
	}

	public void SetColor(Color color){
		this.SetColor(colorID, color);
	}

	public void Apply(){
		this.renderer.SetPropertyBlock(matpropblock);
	}

}