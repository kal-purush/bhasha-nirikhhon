package com.plus.bussinessclusters;

import java.util.Random;
import android.content.Context;
import android.graphics.Bitmap;
import android.util.AttributeSet;
import android.view.animation.AccelerateInterpolator;
import android.view.animation.AlphaAnimation;
import android.view.animation.Animation;
import android.view.animation.Animation.AnimationListener;
import android.view.animation.AnimationSet;
import android.view.animation.DecelerateInterpolator;
import android.widget.ImageView;

public class ImageSlider extends ImageView {

	private Bitmap[] Images=null;
	private int[] ResourceImages=null;
	
	Animation fadeIn = new AlphaAnimation(0, 1);
	Animation fadeOut = new AlphaAnimation(1, 0);

	AnimationSet animation = new AnimationSet(false); // change to false
    int fadeInDuration = 500; // Configure time values here
    int timeBetween = 3000;
    int fadeOutDuration = 1000;
    int ImageIndex=1;
    int resourceIndex=1;
    int minbetween=3000;
    int maxbetween=3000;
    boolean square=false;
    
    //constructors
  	public ImageSlider(Context context) {
  		super(context);
  		// TODO Auto-generated constructor stub
  	}
  	public ImageSlider(Context context, AttributeSet attrs) {
  	   super(context, attrs);
  	}
  	public ImageSlider(Context context, AttributeSet attrs, int defStyle) {
  	   super(context, attrs, defStyle);
  	}

    //private funcs
  	private void CalculateTime()
	{
		//calculate timeBetween
		Random r = new Random();
		int Rnd = r.nextInt(maxbetween-minbetween) + minbetween;
		timeBetween=Rnd;
	}
  	
    //Methods
    public void SetMinShowtime(int time)
    {
    	this.minbetween=time;
    }
    
    public void SetMaxShowtime(int time)
    {
    	this.maxbetween=time;
    }
    
	public void keepSquare(boolean state)
	{
		this.square=state;
	}
	
	public void SetImages(Bitmap[] images)
	{
		this.Images=images;
		setImageBitmap(Images[0]);
	}
	
	public void SetImages(int[] images)
	{
		this.ResourceImages=images;
		setImageResource(images[0]);
	}
	
	
	//start Methods
	private void init()
	{
		 fadeIn.setInterpolator(new DecelerateInterpolator()); // add this
		    fadeIn.setDuration(fadeInDuration);

		  
		    fadeOut.setInterpolator(new AccelerateInterpolator()); // and this
		    fadeOut.setStartOffset(fadeOutDuration+timeBetween);
		    fadeOut.setDuration(fadeOutDuration);

		    
		    animation.addAnimation(fadeIn);
		    animation.addAnimation(fadeOut);
		    animation.setRepeatCount(1);
	}
	
	public void StartBackgroung()
	{
		CalculateTime();
		if(Images.length<=1)
			return;
		
		init();
		    animation.setAnimationListener(new AnimationListener() {
		        public void onAnimationEnd(Animation animation) {
		          
		        	
		        	if(ImageIndex+1>=Images.length)
		        		ImageIndex=0;
		        	else
		        		ImageIndex++;
		        	setImageBitmap(Images[ImageIndex]);
		        	startAnimation(animation);
		        }
		        public void onAnimationRepeat(Animation animation) {
		            // TODO Auto-generated method stub
		        }
		        public void onAnimationStart(Animation animation) {
		            // TODO Auto-generated method stub
		        }
		    });
		   
		    startAnimation(animation);
		   
	}
	
	public void StartResource()
	{
		CalculateTime();
		if(ResourceImages.length<=1)
			return;
	
		init();
		    animation.setAnimationListener(new AnimationListener() {
		        public void onAnimationEnd(Animation animation) {
		          
		        	
		        	setImageResource(ResourceImages[resourceIndex]);
		        	if(resourceIndex+1>=ResourceImages.length)
		        		resourceIndex=0;
		        	else
		        		resourceIndex++;
		        	  
		        	startAnimation(animation);
		        }
		        public void onAnimationRepeat(Animation animation) {
		            // TODO Auto-generated method stub
		        }
		        public void onAnimationStart(Animation animation) {
		            // TODO Auto-generated method stub
		        }
		    });
		    startAnimation(animation);
		
		  
	}
	
	

	//overrides
	 @Override
	    protected void onMeasure(int widthMeasureSpec, int heightMeasureSpec) {
	        super.onMeasure(widthMeasureSpec, heightMeasureSpec);
	        if(this.square)
	        	setMeasuredDimension(getMeasuredWidth(), getMeasuredWidth()); //Snap to width
	    }
}