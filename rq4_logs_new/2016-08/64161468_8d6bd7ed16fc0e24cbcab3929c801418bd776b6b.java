package com.example.luka.openglestest.engine;

import android.content.Context;

import static android.opengl.GLES20.GL_ARRAY_BUFFER;
import static android.opengl.GLES20.GL_FLOAT;
import static android.opengl.GLES20.GL_TEXTURE0;
import static android.opengl.GLES20.GL_TEXTURE_2D;
import static android.opengl.GLES20.GL_TRIANGLES;
import static android.opengl.GLES20.glActiveTexture;
import static android.opengl.GLES20.glBindBuffer;
import static android.opengl.GLES20.glBindTexture;
import static android.opengl.GLES20.glDrawArrays;
import static android.opengl.GLES20.glUniform1f;
import static android.opengl.GLES20.glUniform1i;
import static android.opengl.GLES20.glUniformMatrix4fv;
import static android.opengl.GLES20.glVertexAttribPointer;
import static android.opengl.Matrix.multiplyMM;
import static android.opengl.Matrix.multiplyMV;
import static android.opengl.Matrix.setIdentityM;
import static android.opengl.Matrix.translateM;
import static com.example.luka.openglestest.engine.GLCommon.UseProgram;
import static com.example.luka.openglestest.engine.GLCommon.aNormalLocation;
import static com.example.luka.openglestest.engine.GLCommon.aPositionLocation;
import static com.example.luka.openglestest.engine.GLCommon.aTextureLocation;
import static com.example.luka.openglestest.engine.GLCommon.alphaLocation;
import static com.example.luka.openglestest.engine.GLCommon.modelMatrix;
import static com.example.luka.openglestest.engine.GLCommon.modelMatrixLocation;
import static com.example.luka.openglestest.engine.GLCommon.modelViewProjectionMatrix;
import static com.example.luka.openglestest.engine.GLCommon.modelViewProjectionMatrixLocation;
import static com.example.luka.openglestest.engine.GLCommon.projectionMatrix;
import static com.example.luka.openglestest.engine.GLCommon.renderMode;
import static com.example.luka.openglestest.engine.GLCommon.textureUniformLocation;
import static com.example.luka.openglestest.engine.GLCommon.viewMatrix;
import static com.example.luka.openglestest.engine.GLCommon.viewMatrixLocation;
import static com.example.luka.openglestest.engine.GLCommon.viewProjectionMatrix;

/**
 * Created by Luka on 17.8.2016..
 */
public class GLObjectStatic extends GLObject
{
    public GLObjectStatic (Context context, int resourceId, int textureIDp)
    {
        super( context, resourceId, textureIDp );

        setIdentityM(modelMatrix, 0);
    }

    public GLObjectStatic (GLObjectData object)
    {
        super( object );

        setIdentityM(modelMatrix, 0);
    }

    @Override
    public void Draw( )
    {
        if ( renderMode != GLCommon.renderMode )
        {
            renderModePrevious = GLCommon.renderMode;
            UseProgram( renderMode );
        }

        multiplyMM(viewProjectionMatrix, 0, projectionMatrix, 0, viewMatrix, 0);
        multiplyMM(modelViewProjectionMatrix, 0, viewProjectionMatrix, 0, modelMatrix, 0);

        glUniformMatrix4fv(modelViewProjectionMatrixLocation, 1, false, modelViewProjectionMatrix, 0);
        glUniformMatrix4fv(viewMatrixLocation, 1, false, viewMatrix, 0);
        glUniformMatrix4fv(modelMatrixLocation, 1, false, modelMatrix, 0);

        glUniform1f( alphaLocation, alpha );

        glBindTexture(GL_TEXTURE_2D, textureID);

        glActiveTexture(GL_TEXTURE0);

        glUniform1i(textureUniformLocation, 0); // ovo se mozda treba raditi samo kod inita shadera

        glBindBuffer(GL_ARRAY_BUFFER, bufferIndex);
        glVertexAttribPointer(aPositionLocation, VERTEX_DATA_SIZE, GL_FLOAT, false, stride, 0);

        glBindBuffer(GL_ARRAY_BUFFER, bufferIndex);
        glVertexAttribPointer(aNormalLocation, NORMAL_DATA_SIZE, GL_FLOAT, false, stride, VERTEX_DATA_SIZE * BYTES_PER_FLOAT);

        glBindBuffer(GL_ARRAY_BUFFER, bufferIndex);
        glVertexAttribPointer(aTextureLocation, UV_DATA_SIZE, GL_FLOAT, false,
                stride, (VERTEX_DATA_SIZE + NORMAL_DATA_SIZE) * BYTES_PER_FLOAT);

        glBindBuffer(GL_ARRAY_BUFFER, 0);

        glDrawArrays(GL_TRIANGLES, 0, vertices.length / 3);

    }

    @Override
    public void Translate( float x, float y, float z )
    {
        translateM(modelMatrix, 0, x, y, z);
        translateM(translationMatrix, 0, x, y, z);
        multiplyMV(position, 0, translationMatrix, 0, initPosition, 0);
    }

    @Override
    public void TranslateTo( float x, float y, float z )
    {
        setIdentityM(translationMatrix, 0);
        translateM(translationMatrix, 0, x, y, z);
        multiplyMV(position, 0, translationMatrix, 0, initPosition, 0);

        setIdentityM(modelMatrix, 0);
        multiplyMM(modelMatrix, 0, scaleMatrix, 0, modelMatrix, 0); // 1. ili 3.?
        multiplyMM(modelMatrix, 0, rotationMatrix, 0, modelMatrix, 0); // 2.
        multiplyMM(modelMatrix, 0, translationMatrix, 0, modelMatrix, 0); // 1. ili 3.?
    }
}