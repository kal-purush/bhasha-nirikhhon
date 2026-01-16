package dataMapping;

import static java.awt.Font.TRUETYPE_FONT;
import static java.awt.Font.createFont;

import java.awt.FontFormatException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.newdawn.slick.Font;
import org.newdawn.slick.TrueTypeFont;
import org.newdawn.slick.openal.Audio;
import org.newdawn.slick.openal.AudioLoader;
import org.newdawn.slick.opengl.Texture;
import org.newdawn.slick.opengl.TextureLoader;
import org.newdawn.slick.util.ResourceLoader;

public class Data {

	public static String[] folders = new String[]{"fonts", "images/skins", "images/textures", "sounds"};

	public static Map<String , Audio> soundsMap = new HashMap<>();
    public static Map<String , Font> fontsMap = new HashMap<>();
    public static Map<String , Texture> texturesMap = new HashMap<>();
    public static Map<String , Texture[]> skinsMap = new HashMap<>();

    static {
    	for(String folder: folders) {
			try {
				Stream<Path> paths;
				paths = Files.walk(Paths.get(folder));
				paths.forEach(filePath -> {
		            if (Files.isRegularFile(filePath)) {
		            	String fileName = getFileName(filePath);

		            	switch(folder) {

		                case "fonts":
		                	try {
			                	fontsMap.put(fileName, createTTF(folder, fileName));
							} catch (FontFormatException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
		                	break;

		                case "images/textures":
		                	try {
			                	texturesMap.put(fileName, createTexture(folder, fileName));
							} catch (IOException e) {
								e.printStackTrace();
							}
		                	break;

		                case "images/skins":
		                	try {
		                		if(!fileName.contains("_b"))
		                			skinsMap.put(fileName, createSkin(folder, fileName));
							} catch (IOException e) {
								e.printStackTrace();
							}
		                	break;

		                case "sounds":
		                	try {
		                		soundsMap.put(fileName, createAudio(folder, fileName));
							} catch (IOException e) {
								e.printStackTrace();
							}
		                	break;

		                }

		            }
		        });
				paths.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }

    }

    public static Audio createAudio(String folder, String fileName) throws IOException {
    	InputStream stream = ResourceLoader.getResourceAsStream(folder + "/" + fileName + ".ogg");
    	return AudioLoader.getAudio("OGG", stream);
    }

    public static Texture createTexture(String folder, String fileName) throws IOException {
    	InputStream stream = ResourceLoader.getResourceAsStream(folder + "/" + fileName + ".png");
    	return TextureLoader.getTexture("PNG", stream);
    }

    public static TrueTypeFont createTTF(String folder, String fileName) throws FontFormatException, IOException {
    	InputStream stream = ResourceLoader.getResourceAsStream(folder + "/" + fileName + ".ttf");
		return new TrueTypeFont(createFont(TRUETYPE_FONT, stream).deriveFont(46f), false);
    }

    public static Texture[] createSkin(String folder, String fileName) throws IOException {
    	InputStream stream = ResourceLoader.getResourceAsStream(folder + "/" + fileName + ".png");
    	InputStream stream2 = ResourceLoader.getResourceAsStream(folder + "/" + fileName + "_b.png");
    	return new Texture[]{TextureLoader.getTexture("PNG", stream) , TextureLoader.getTexture("PNG", stream2)};
    }

    public static String getFileName(Path filePath) {
    	String str = filePath.toString().split("\\.")[0];
    	String[] array = str.split("/");
    	if(array.length > 1) {
    		//Cas où le chemin est de la forme : "dossier1/dossier2/.../fichier.extension" (Unix)
    		return array[array.length - 1];
    	} else {
    		//Cas où le chemin est de la forme : "dossier1\\dossier2\\...\\fichier.extension" (Windows)
    		array = str.split("\\\\");
    		return array[array.length - 1];
    	}
    }

}