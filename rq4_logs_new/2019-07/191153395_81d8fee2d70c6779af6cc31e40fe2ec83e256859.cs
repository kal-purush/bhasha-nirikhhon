/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Filename: SlideShow.cs
//  
// Author: Garth "Corrupted Heart" de Wet <mydeathofme[at]gmail[dot]com>
// Website: www.CorruptedHeart.co.cc
// 
// Copyright (c) 2010 Garth "Corrupted Heart" de Wet
//  
// Permission is hereby granted, free of charge (a donation is welcome at my website), to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

using UnityEngine;
using System.Collections;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

[RequireComponent(typeof(GUITexture))]
public class SlideShow : MonoBehaviour
{

    public Animator animator;


    public Sprite[] slides = new Sprite[1];
    public float changeTime = 10.0f;
    private int currentSlide = 0;
    private float timeSinceLast = 1.0f;
    public GameObject ImageGameObject;

    void Start()
    {
        /*
        guiTexture.texture = slides[currentSlide];
        image.pixelInset = new Rect(-slides[currentSlide].width / 2, -slides[currentSlide].height / 2, slides[currentSlide].width, slides[currentSlide].height);
        */
        ImageGameObject.GetComponent<Image>().sprite = slides[currentSlide];
        currentSlide++;
    }

    void Update()
    {
        if (timeSinceLast > changeTime && currentSlide < slides.Length)
        {
            ImageGameObject.GetComponent<Image>().sprite = slides[currentSlide];
            timeSinceLast = 0.0f;
            currentSlide++;
        }
        // comment out this section if you don't want the slide show to loop
        // -----------------------
        else if (timeSinceLast > changeTime && currentSlide == slides.Length)
        {
            //TODO: Change scene

            SceneTransition.setScene("Mainmenu");
            SceneTransition.getScene();
            StartCoroutine(SceneTransition.LoadScene());        }
        // ------------------------
        timeSinceLast += Time.deltaTime;
    }

}