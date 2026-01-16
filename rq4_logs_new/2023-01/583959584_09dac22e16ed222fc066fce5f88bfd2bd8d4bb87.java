package com.github.industrialcraft.folder;

import com.badlogic.gdx.files.FileHandle;
import com.badlogic.gdx.graphics.Pixmap;
import com.badlogic.gdx.graphics.PixmapIO;
import com.badlogic.gdx.graphics.Texture;
import com.badlogic.gdx.graphics.g2d.TextureRegion;
import com.badlogic.gdx.utils.JsonReader;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class SaverLoader {
    public static JsonObject toJson(Node rootNode){
        return toJson(rootNode, new AtomicInteger(0), null);
    }
    private static JsonObject toJson(Node rootNode, AtomicInteger textureIdGen, HashMap<Integer,Texture> textures){
        JsonObject json = new JsonObject();
        json.addProperty("name", rootNode.name);
        json.addProperty("originX", rootNode.nodeTexture.xOrigin);
        json.addProperty("originY", rootNode.nodeTexture.yOrigin);
        if(rootNode.nodeTexture.getTexture() != null) {
            json.addProperty("texture", textureIdGen == null ? rootNode.nodeTexture.getTexturePath() : ("t" + textureIdGen.incrementAndGet()));
            if (textureIdGen != null && textures != null)
                textures.put(textureIdGen.get(), rootNode.nodeTexture.getTexture());
        }
        JsonObject animations = new JsonObject();
        for(var entry : rootNode.animations.entrySet()){
            JsonArray animation = new JsonArray();
            for(Animation.TransformWithLength transform : entry.getValue().transforms){
                JsonObject jsonTransform = new JsonObject();
                jsonTransform.addProperty("length", transform.length);
                jsonTransform.addProperty("x", transform.transform.x);
                jsonTransform.addProperty("y", transform.transform.y);
                jsonTransform.addProperty("rotation", transform.transform.rotation);
                jsonTransform.addProperty("size", transform.transform.size);
                jsonTransform.addProperty("opacity", transform.transform.opacity);
                animation.add(jsonTransform);
            }
            animations.add(entry.getKey(), animation);
        }
        json.add("animations", animations);
        JsonArray children = new JsonArray();
        for(Node child : rootNode.getChildren()){
            children.add(toJson(child, textureIdGen, textures));
        }
        json.add("children", children);
        return json;
    }
    public static Node fromJson(JsonObject jsonObject, Function<String, Texture> textureResolver){
        Node node = new Node();
        node.name = jsonObject.get("name").getAsString();
        node.nodeTexture.xOrigin = jsonObject.get("originX").getAsFloat();
        node.nodeTexture.yOrigin = jsonObject.get("originY").getAsFloat();
        JsonElement texture = jsonObject.get("texture");
        if(texture != null){
            node.nodeTexture.setTexture(textureResolver.apply(texture.getAsString()), texture.getAsString());
        }
        JsonObject animations = jsonObject.getAsJsonObject("animations");
        for(var anim : animations.asMap().entrySet()){
            JsonArray transforms = anim.getValue().getAsJsonArray();
            Animation animation = new Animation();
            for(var e : transforms){
                JsonObject lengthTransform = e.getAsJsonObject();
                float x = lengthTransform.get("x").getAsFloat();
                float y = lengthTransform.get("y").getAsFloat();
                float rotation = lengthTransform.get("rotation").getAsFloat();
                float size = lengthTransform.get("size").getAsFloat();
                float opacity = lengthTransform.get("opacity").getAsFloat();
                Transform transform = new Transform(x, y, rotation, size, opacity);
                animation.transforms.add(new Animation.TransformWithLength(transform, lengthTransform.get("length").getAsFloat()));
            }
            node.animations.put(anim.getKey(), animation);
        }
        JsonArray children = jsonObject.get("children").getAsJsonArray();
        for(var child : children){
            node.addChild(fromJson(child.getAsJsonObject(), textureResolver));
        }
        return node;
    }
    public static void exportZip(File file, Node rootNode) throws IOException {
        HashMap<Integer,Texture> textures = new HashMap<>();
        ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(file));
        zip.putNextEntry(new ZipEntry("renderdata.json"));
        zip.write(toJson(rootNode, new AtomicInteger(0), textures).toString().getBytes(StandardCharsets.UTF_8));
        zip.closeEntry();
        for(var e : textures.entrySet()){
            zip.putNextEntry(new ZipEntry("t" + e.getKey() + ".png"));
            Texture texture = e.getValue();
            if (!texture.getTextureData().isPrepared()) {
                texture.getTextureData().prepare();
            }
            Pixmap pixmap = texture.getTextureData().consumePixmap();
            PixmapIO.PNG pngWriter = new PixmapIO.PNG((int)(pixmap.getWidth() * pixmap.getHeight() * 1.5f)); //approximated deflated size
            pngWriter.setFlipY(false);
            pngWriter.write(zip, pixmap);
            zip.closeEntry();
        }
        zip.close();
    }
    public static Node loadZip(File zip) throws IOException {
        ZipFile zipFile = new ZipFile(zip);
        var entries = zipFile.entries();
        HashMap<String,Texture> textures = new HashMap<>();
        while(entries.hasMoreElements()){
            ZipEntry entry = entries.nextElement();
            if(!entry.getName().endsWith(".png"))
                continue;
            InputStream stream = zipFile.getInputStream(entry);
            textures.put(entry.getName().replace(".png", ""), new Texture(new FileHandle(""){
                @Override
                public InputStream read() {
                    return stream;
                }
            }));
        }
        return fromJson(JsonParser.parseString(new String(zipFile.getInputStream(zipFile.getEntry("renderdata.json")).readAllBytes())).getAsJsonObject(), textures::get);
    }
}