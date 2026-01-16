package com.example.rockpaperscissors;

import java.util.Random;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.view.animation.Animation;
import android.view.animation.Animation.AnimationListener;
import android.view.animation.AnimationUtils;
import android.widget.Button;
import android.widget.ImageButton;
import android.widget.ImageView;
import android.widget.TextView;

public class Outlet extends Activity implements android.view.View.OnClickListener, AnimationListener
{
	 public enum Option {
		  ROCK, PAPER, SCISSORS
		 }
		  
		 public enum Result {
		  WIN, LOSS, DRAW  
		 }
		 private Option userSelection,androidSelection;
		 int uc=0;
		 int ac=0;
		 Button bt1,bt2,bt3;
		 ImageButton bt4;
		 Animation animalpha;
	 @Override
	    protected void onCreate(Bundle savedInstanceState) {
	        super.onCreate(savedInstanceState);
	        setContentView(R.layout.activity_another);
	         bt1=(Button)findViewById(R.id.rock);
	         bt2=(Button)findViewById(R.id.paper);
	         bt3=(Button)findViewById(R.id.scissors);
	         bt4 = (ImageButton)findViewById(R.id.imageButtonHome);
	         //animalpha = AnimationUtils.loadAnimation(getApplicationContext(), R.anim.activity_alpha);
	        bt1.setOnClickListener(this);
	        bt2.setOnClickListener(this);
	        bt3.setOnClickListener(this);
	        bt4.setOnClickListener(this);
	        //animalpha.setAnimationListener(this);
	    }

	@Override
	public void onClick(View v) {
		// TODO Auto-generated method stub
		
		
		
		
		ImageView imageView = (ImageView) findViewById(R.id.user);
		
		  boolean play = true;
		   
		  switch (v.getId()) {
		   case R.id.rock:
			  // bt1.startAnimation(animalpha);
		    userSelection = Option.ROCK;
		    imageView.setImageResource(R.drawable.rock);
		    
//		    bt2.clearAnimation();
//			  bt3.clearAnimation();
		    break; 
		   case R.id.paper:
			  // bt2.startAnimation(animalpha);
		    userSelection = Option.PAPER;
		    imageView.setImageResource(R.drawable.paper);
//		    bt3.clearAnimation();
//			  bt1.clearAnimation();
		    break;
		   case R.id.scissors:
			  // bt3.startAnimation(animalpha);
		    userSelection = Option.SCISSORS;
		    imageView.setImageResource(R.drawable.scissors);
//		    bt2.clearAnimation();
//			  bt1.clearAnimation();
		    break;
		    
		   case R.id.imageButtonHome:
			  // bt4.startAnimation(animalpha);
			   Intent i = new Intent(Outlet.this, MainActivity.class);
			    startActivity(i); // To go home.
			    play = false;
//			    bt3.clearAnimation();
//				  bt1.clearAnimation();
//				  bt2.clearAnimation();
				  
			    break;
		   
		  }
		  
		  
		  
		   
		  if(play) {
		   play();
		   if(uc==10 || ac==10)
		   showResults();
		  }
	}

	 private void showResults() {
		  AlertDialog.Builder builder = new AlertDialog.Builder(Outlet.this);     
		     builder.setCancelable(false);     
		     builder.setPositiveButton("OK", new DialogInterface.OnClickListener() {   
		   @Override
		   public void onClick(DialogInterface dialog, int which) {
		    // Do nothing  
			   ImageView imageView = (ImageView) findViewById(R.id.user);
			   ImageView imageView1 = (ImageView) findViewById(R.id.android);
			   imageView.setImageResource(R.drawable.images);
			   imageView1.setImageResource(R.drawable.images);
			   TextView userS = (TextView) findViewById(R.id.textView2);
		         TextView androidS = (TextView) findViewById(R.id.textView3);
			   uc=0;
			   ac=0;
			   userS.setText(String.valueOf("user score:"+uc));
				 androidS.setText(String.valueOf("Computer Score:"+ac));
		   }
		  });
		      
		     // Sets the right message according to result.
		     if(ac>uc) {
		      builder.setMessage("You Loose!");
		     } else if(ac<uc) {
		      builder.setMessage("You Win!");
		     } else if(ac==uc) {
		      builder.setMessage("It's a draw!");
		     } 
		      
		     AlertDialog alert = builder.create();
		     alert.show();  
		    } 
		 
		 private void play() {
		  // Generates a random play.
			 TextView userS = (TextView) findViewById(R.id.textView2);
	         TextView androidS = (TextView) findViewById(R.id.textView3);
		  Random random =  new Random();
		  int rand=random.nextInt(3);
		  androidSelection = null;
		  ImageView imageView = (ImageView) findViewById(R.id.android);
		   
		  // Sets the right image according to random selection.
		  switch (rand) {
		   case 0:
		    androidSelection = Option.ROCK;
		    imageView.setImageResource(R.drawable.rock);
		    break;
		   case 1:
		    androidSelection = Option.PAPER;
		    imageView.setImageResource(R.drawable.paper);
		    break;
		   case 2:
		    androidSelection = Option.SCISSORS;
		    imageView.setImageResource(R.drawable.scissors);
		    break;
		  }
		  // Determine game result according to user selection and Android selection.
		  if(androidSelection == userSelection) {
		   uc++;
		   ac++;
		  }
		  else if(androidSelection == Option.ROCK && userSelection == Option.SCISSORS) {

		   ac++;
		  }
		  else if(androidSelection == Option.PAPER && userSelection == Option.ROCK) {

		   ac++;
		  } 
		  else if(androidSelection == Option.SCISSORS && userSelection == Option.PAPER) {

		   ac++;
		  } else {
			  
		   uc++;
		  } 
		 userS.setText(String.valueOf("user score:"+uc));
		 androidS.setText(String.valueOf("Computer Score:"+ac));
		 }

		@Override
		public void onAnimationEnd(Animation arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onAnimationRepeat(Animation arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onAnimationStart(Animation arg0) {
			// TODO Auto-generated method stub
			
		}    
		 @Override
		   public void onBackPressed() {
			 Intent back = new Intent(Outlet.this, MainActivity.class);
			    startActivity(back);
		      
		   }
		}

	

	
