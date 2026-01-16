using Godot;
using System;
using System.Collections.Generic;

public static class PaletteManager {
	
	private static Dictionary<string, Texture2D> fetchedPalettes = new Dictionary<string, Texture2D>();
	
	private static Texture2D emptyPalette = null;
	
	//Methods
	
	private static Texture2D CreateEmptyPalette()
	{
		//Hexadecimal of 10 is in fact, 0x0A
		byte[] pixel = new byte[] { 0xEF, 0xE2, 0xDD, 0x00 };
		
		Image emptyImage = Image.CreateFromData(1, 1, false, Image.Format.Rgba8, pixel);
		
		var textureImg = ImageTexture.CreateFromImage(emptyImage);
		
		return textureImg as Texture2D;
	}
	
	public static Texture2D FetchEmptyPalette()
	{
		if (emptyPalette == null)
			emptyPalette = CreateEmptyPalette();
		return emptyPalette; 
	}
	
	public static Texture2D FetchPalette(string name)
	{
		//Lazy initialize the empty Texture2D
		if (emptyPalette == null)
			emptyPalette = CreateEmptyPalette();
		
		if (fetchedPalettes.ContainsKey(name))
		{
			return fetchedPalettes[name];
		}
		
		//Try to load the Bitmap
		
		BMP image = new BMP();
		
		image.LoadFile("./Resource/palettes/"+name+".bmp");
		
		Texture2D pal = image.GetPalette();
		
		//if it fails, then cache it as an empty texture
		if (pal == null)
			pal = emptyPalette;
			
		fetchedPalettes.Add(name, pal);
		
		return pal;
	}

}
