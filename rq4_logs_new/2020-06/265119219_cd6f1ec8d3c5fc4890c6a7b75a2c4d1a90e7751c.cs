using System.Collections;
using System.Collections.Generic;
using UnityEngine;


namespace Pixar
{
	[ExecuteInEditMode]
    [RequireComponent(typeof(Camera))]
	public class ScanEffect : MonoBehaviour
	{
		Material _material;
		Shader _shader;
		Camera _camera;

		void Awake()
		{
			_camera = GetComponent<Camera>();
			_camera.depthTextureMode = DepthTextureMode.Depth;
		}
		
	    private void OnRenderImage(RenderTexture source, RenderTexture destionation)
	    {
	    	if(_material == null)
	    	{
	    		_shader = Shader.Find("Pixar/ScanEffectShader");
	    		_material = new Material(_shader);
	    		_material.hideFlags = hideFlags.DontSave;
	    	}
	    	// _material.SetFloat();
	    	Graphics.Blit(source, destionation, _material);
	    }
	}

}
