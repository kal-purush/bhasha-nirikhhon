package com.example.luka.openglestest.engine;

import android.content.Context;

import com.example.luka.openglestest.util.TextResourceReader;

import java.util.List;


//  Sluzi za ucitavanje podataka o tipu objekta (vrhova, normala, UV koordinata i teksture),
//  da se oni ne bi morali ucitavati kod svakog instanciranja razreda GLObject
//
//  GLObjekt se moze instancirati tako da mu se kao parametar da objekt ovog razreda


public class GLObjectData
{
    float[] vertices, normals, UVs;

    public int textureID;

    public GLObjectData() {}

    public GLObjectData(Context context, int resourceId, int textureIDp )
    {
        List tempList = TextResourceReader.LoadObjFromResource( context, resourceId );
        vertices =  (float[]) tempList.get(0);
        normals = (float[]) tempList.get(1);
        UVs = (float[]) tempList.get(2);

        textureID = textureIDp;
    }
}