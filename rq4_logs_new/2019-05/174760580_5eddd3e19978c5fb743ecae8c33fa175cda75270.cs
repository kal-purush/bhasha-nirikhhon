using UnityEditor;

[InitializeOnLoad]
public class WebGLPlayerSettings
{
    static WebGLPlayerSettings()
    {
        PlayerSettings.WebGL.linkerTarget = WebGLLinkerTarget.Wasm;
        PlayerSettings.WebGL.memorySize = 512;
    }
}