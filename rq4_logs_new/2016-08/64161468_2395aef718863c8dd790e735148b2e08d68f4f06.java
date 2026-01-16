package com.example.luka.openglestest.engine;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.opengl.GLUtils;

import com.example.luka.openglestest.ShaderHelper;
import com.example.luka.openglestest.util.TextResourceReader;

import static android.opengl.GLES20.*;

/**
 * Created by Luka on 1.8.2016..
 */
public class GLCommon
{
    public static int program;

    public static float[] modelMatrix = new float[16];
    public static int modelMatrixLocation;
    public static final float[] viewMatrix = new float[16];
    public static int viewMatrixLocation;
    public static final float[] projectionMatrix = new float[16];
    public static int projectionMatrixLocation;
    public static final float[] viewProjectionMatrix = new float[16];

    public static final float[] modelViewProjectionMatrix = new float[16];
    public static int modelViewProjectionMatrixLocation;

    public static float[] lightPosition = {0.0f, 1.0f, 1.0f};
    public static int lightPositionLocation;

    public static float[] eyePosition = {1.0f, 1.0f, 1.0f};
    public static int eyePositionLocation;

    public static float[] center = {0.0f, 0.0f, 0.0f};

    public static int aPositionLocation, aNormalLocation, aTextureLocation;
    public static int textureUniformLocation;

    public static void InitShaders(Context context, int vertexShaderId, int fragmentShaderId)
    {
        String vertexShaderSource = TextResourceReader.readTextFileFromResource(context, vertexShaderId);
        String fragmentShaderSource = TextResourceReader.readTextFileFromResource(context, fragmentShaderId);

        int vertexShader = ShaderHelper.compileVertexShader(vertexShaderSource);
        int fragmentShader = ShaderHelper.compileFragmentShader(fragmentShaderSource);
        program = ShaderHelper.linkProgram(vertexShader, fragmentShader);

        glUseProgram(program);

        aPositionLocation = glGetAttribLocation(program, "a_Position");
        aNormalLocation = glGetAttribLocation(program, "a_Normal");
        aTextureLocation = glGetAttribLocation(program, "a_Texture");
        textureUniformLocation = glGetUniformLocation(program, "u_Texture");

        viewMatrixLocation = glGetUniformLocation(program, "V");
        modelMatrixLocation = glGetUniformLocation(program, "M");
        modelViewProjectionMatrixLocation = glGetUniformLocation(program, "MVP");

        lightPositionLocation = glGetUniformLocation(program, "LightPosition_worldspace");
        eyePositionLocation = glGetUniformLocation(program, "ociste");

        glEnableVertexAttribArray(GLCommon.aPositionLocation);
        glEnableVertexAttribArray(GLCommon.aNormalLocation);
        glEnableVertexAttribArray(GLCommon.aTextureLocation);
    }

    public static int LoadTexture(int resourceName, Context context)
    {
        final int[] textureHandle = new int[1];

        glGenTextures(1, textureHandle, 0);

        final BitmapFactory.Options options = new BitmapFactory.Options();
        options.inScaled = false;   // No pre-scaling

        // Read in the resource
        //int resID = context.getResources().getIdentifier(resourceName, "raw", context.getPackageName());
        final Bitmap bitmap = BitmapFactory.decodeResource(context.getResources(), resourceName, options);

        // Bind to the texture in OpenGL
        glBindTexture(GL_TEXTURE_2D, textureHandle[0]);

        // Set filtering
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_NEAREST);
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAG_FILTER, GL_NEAREST);

        // Load the bitmap into the bound texture.

        GLUtils.texImage2D(GL_TEXTURE_2D, 0, bitmap, 0);

        // Recycle the bitmap, since its data has been loaded into OpenGL.
        bitmap.recycle();

        return textureHandle[0];
    }

}