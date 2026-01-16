//---------------------------------------------------------------------------------------
// Copyright 2016 North Carolina State University
//
// Center for Educational Informatics
// http://www.cei.ncsu.edu/
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//   * Redistributions of source code must retain the above copyright notice, this 
//     list of conditions and the following disclaimer.
//   * Redistributions in binary form must reproduce the above copyright notice, 
//     this list of conditions and the following disclaimer in the documentation 
//     and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE 
// GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
// OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
//---------------------------------------------------------------------------------------

using UnityEngine;
using System.Collections;

namespace IntelliMedia
{
	public class TextureRecorder : MonoBehaviour {

		public RenderTexture renderTexture;	
		public float frameRate = 30;
		public bool isRecording;

		private string outputDirectory;
		private int frame;
		private float lastProcessTime;
		private Texture2D outputTexture;

		public void Start()
		{
			Contract.PropertyNotNull("renderTexture", renderTexture);
			outputTexture = new Texture2D(renderTexture.width, renderTexture.height, TextureFormat.RGB24, false);
		}

		public void StartRecording(string directory)
		{
			Contract.ArgumentNotNull("directory", directory);
			outputDirectory = directory;

			frame = 0;

			DebugLog.Info("Start recording frames to: {0}", directory);

			System.IO.Directory.CreateDirectory(outputDirectory);
			isRecording = true;
		}

		public void EndRecording()
		{
			DebugLog.Info("End recording");
		
			isRecording = false;
		}

		void Update () 
		{
			if (!isRecording || (1/frameRate > Time.time - lastProcessTime))
			{
				return;
			}

			WriteTextureToFile();
			lastProcessTime = Time.time; 
		}

		private void WriteTextureToFile()
		{
			string name = System.IO.Path.Combine(outputDirectory, string.Format("Frame{0:D5}.jpeg", frame++)); 
			RenderTexture.active = renderTexture;	
			outputTexture.ReadPixels(new Rect(0, 0, renderTexture.width, renderTexture.height), 0, 0);
			outputTexture.Apply();
			byte[] image = outputTexture.EncodeToJPG();	
			System.IO.File.WriteAllBytes(name, image);
		}
	}
}