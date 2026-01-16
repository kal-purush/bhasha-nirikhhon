using Godot;
using System;
using System.Collections.Generic;

//To Do: re-think if this class should inherit from Node2D
public partial class TextureAtlas : SubViewport
{
	private ImmediateMesh immediateMesh;
	//To do: Merge this with this.Material
	private ShaderMaterial material;
	
	private Texture2D texture;
	private Texture2D palette;
	
	private MeshInstance2D dummyMesh;
	
	private bool cachedTexture = false;
	
	public override void _Ready()
	{
		Size = new Vector2I(1024, 64);
		
		texture = TextureManager.FetchTexture("./art/textures/flower.bmp");
		palette = GD.Load<Texture2D>("res://Resource/palettes/petzpalette.png");
		
		dummyMesh = new MeshInstance2D();
		
		immediateMesh = new ImmediateMesh();
		material = ShaderManager.FetchShaderMaterial("texture_atlas/texture");
		
		dummyMesh.Mesh = immediateMesh;
		dummyMesh.Material = material;
		
		immediateMesh.ClearSurfaces();
		immediateMesh.SurfaceBegin(Mesh.PrimitiveType.Triangles);
		
		immediateMesh.SurfaceAddVertex(new Vector3(0, 0, 0));
		immediateMesh.SurfaceAddVertex(new Vector3(0, 64, 0));
		immediateMesh.SurfaceAddVertex(new Vector3(64, 64, 0));
		
		immediateMesh.SurfaceAddVertex(new Vector3(0, 0, 0));
		immediateMesh.SurfaceAddVertex(new Vector3(64, 0, 0));
		immediateMesh.SurfaceAddVertex(new Vector3(64, 64, 0));
		
		immediateMesh.SurfaceEnd();
		
		material.SetShaderParameter("tex", texture);
		material.SetShaderParameter("palette", palette);
		
		/*await RenderingServer.FramePostDraw;
		if (cachedTexture == false){
			textureAtlas.GetTexture().GetImage().SavePng("res://cache/texture_atlas/1.png");
			cachedTexture = true;
		}*/
	}
	
	// CUSTOM METHODS
	
	public Texture2D GetTexture2D(){
		
		return this.GetTexture() as Texture2D;
	}
}