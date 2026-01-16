package c4.subnetzero.shipsdroid;


import android.annotation.SuppressLint;
import android.app.ActionBar;
import android.app.Activity;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.graphics.drawable.Drawable;
import android.os.Bundle;
import android.os.Handler;
import android.os.IBinder;
import android.os.Message;
import android.util.DisplayMetrics;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageButton;
import android.widget.TextView;
import c4.subnetzero.shipsdroid.controller.GameEngine;
import c4.subnetzero.shipsdroid.view.EnemyFleetView;
import c4.subnetzero.shipsdroid.view.GridButtonHandler;
import c4.subnetzero.shipsdroid.view.OwnFleetView;

public class GameActivity extends Activity implements Handler.Callback, ServiceConnection
{

   private static final String LOG_TAG = "GameActivity";

   private Handler mUiHandler;
   private Menu mMenu;
   private EnemyFleetView mEnemyFleetView;
   private OwnFleetView mOwnFleetView;
   private GameEngine mGameEngine;
   private NetService mNetService;
   private GridButtonHandler mGridButtonHandler;
   private TextView mShotClockView;
   private TextView mEnemyShipsView;
   private TextView mMyShipsView;
   private ImageButton mConState;
   //private ActionBar mActionBar;

   public static final int UPDATE_SHOT_CLOCK = 1;
   public static final int UPDATE_SCORE_BOARD = 2;
   public static final int UPDATE_GAME_MENU = 3;
   public static final int UPDATE_CONNECTION_STATE = 4;
   public static final int PEER_QUIT_APP = 5;

   @Override
   public void onCreate(Bundle savedInstanceState)
   {
      Log.d(LOG_TAG, "onCreate()");
      super.onCreate(savedInstanceState);
      setContentView(R.layout.game);

      mUiHandler = new Handler(this);
      //mRestarted = false;
      setup();
   }

   @Override
   protected void onRestart()
   {
      Log.d(LOG_TAG, "onRestart()");
      super.onRestart();
      //mRestarted = true;
   }

   @Override
   protected void onStart()
   {
      Log.d(LOG_TAG, "onStart()");
      super.onStart();
   }


   @Override
   protected void onResume()
   {
      Log.d(LOG_TAG, "onResume()");
      super.onResume();

      if (mGameEngine != null) {
         mGameEngine.setHidden(false);
      }
   }


   @Override
   protected void onPause()
   {
      Log.d(LOG_TAG, "onPause()");
      super.onPause();

      if (mGameEngine != null) {
         if (mGameEngine.getStateName().equals("Playing")) {
            mGameEngine.pauseGame();
         } else {
            mGameEngine.setHidden(true);
         }
      }
   }

   @Override
   protected void onDestroy()
   {
      Log.d(LOG_TAG, "onDestroy()");
      if (mGameEngine != null) {
         mGameEngine.shutDown();
      }
      unbindService(this);
      super.onDestroy();
   }

   @Override
   public void onBackPressed()
   {
      quitApp();
      finish();
   }

   @Override
   protected void onRestoreInstanceState(Bundle savedInstanceState)
   {
      Log.d(LOG_TAG, "onRestoreInstanceState()");
      super.onRestoreInstanceState(savedInstanceState);
   }

   @Override
   protected void onSaveInstanceState(Bundle outState)
   {
      Log.d(LOG_TAG, "onSaveInstanceState()");
      super.onSaveInstanceState(outState);
   }


   // Now the size of the views should be available
   @Override
   public void onWindowFocusChanged(boolean hasFocus)
   {
      super.onWindowFocusChanged(hasFocus);

      if (mEnemyFleetView == null) {
         buildGameBoard();
         mUiHandler.sendEmptyMessageDelayed(UPDATE_CONNECTION_STATE, 2000);
      }
   }

   @Override
   public boolean onCreateOptionsMenu(Menu menu)
   {
      MenuInflater inflater = getMenuInflater();
      inflater.inflate(R.menu.game, menu);
      mMenu = menu;
      return true;
   }

   @Override
   public boolean onOptionsItemSelected(MenuItem item)
   {
      switch (item.getItemId()) {
         case R.id.new_game:
            mGameEngine.newGame();
            break;
         case R.id.pause_game:
            mGameEngine.pauseGame();
            break;
         case R.id.resume_game:
            mGameEngine.resumeGame();
            break;
         case R.id.abort_game:
            mGameEngine.abortGame();
            break;
         case R.id.quit_game_app:
            quitApp();
            finish();
            break;
      }

      return true;
   }

   @Override
   public boolean handleMessage(Message msg)
   {
      switch (msg.what) {
         case UPDATE_SHOT_CLOCK:
            mShotClockView.setText(String.valueOf(msg.arg1));
            break;
         case UPDATE_SCORE_BOARD:
            mMyShipsView.setText(String.valueOf(msg.arg1));
            mEnemyShipsView.setText(String.valueOf(msg.arg2));
            break;
         case UPDATE_GAME_MENU:
            switch (mGameEngine.getStateName()) {
               case "PeerReady":
                  mMenu.findItem(R.id.new_game).setVisible(true);
                  mMenu.findItem(R.id.pause_game).setVisible(false);
                  mMenu.findItem(R.id.resume_game).setVisible(false);
                  mMenu.findItem(R.id.abort_game).setVisible(false);
                  break;
               case "Playing":
                  mMenu.findItem(R.id.new_game).setVisible(false);
                  mMenu.findItem(R.id.pause_game).setVisible(true);
                  mMenu.findItem(R.id.resume_game).setVisible(false);
                  mMenu.findItem(R.id.abort_game).setVisible(true);
                  break;
               case "Paused":
                  mMenu.findItem(R.id.new_game).setVisible(false);
                  mMenu.findItem(R.id.pause_game).setVisible(false);
                  mMenu.findItem(R.id.resume_game).setVisible(true);
                  mMenu.findItem(R.id.abort_game).setVisible(true);
                  break;
               default:
                  break;
            }
            break;
         case UPDATE_CONNECTION_STATE:
            Drawable stateDrawable;
            switch (mNetService.getState()) {
               case CONNECTED:
               case UNREACHABLE:
                  stateDrawable = getResources().getDrawable(R.drawable.state_yellow_bg);
                  break;
               case DISCONNECTED:
                  stateDrawable = getResources().getDrawable(R.drawable.state_red_bg);
                  break;
               case REACHABLE:
                  stateDrawable = getResources().getDrawable(R.drawable.state_green_bg);
                  break;
               default:
                  stateDrawable = getResources().getDrawable(R.drawable.state_gray_bg);
                  break;
            }
            mConState.setBackground(stateDrawable);
            break;
         case PEER_QUIT_APP:
            finish();
         default:
            /* EMPTY */
            break;

      }
      return true;
   }

   @Override
   public void onServiceConnected(ComponentName name, IBinder service)
   {
      Log.d(LOG_TAG, "onServiceConnected()");
      mNetService = ((NetService.LocalBinder) service).getService();

      mGameEngine = new GameEngine(this, mNetService);
      mGameEngine.setModelUpdateListener(mOwnFleetView, mEnemyFleetView);

      mGridButtonHandler.setGameEngine(mGameEngine);
   }


   // Only gets called when service has crashed!!
   @Override
   public void onServiceDisconnected(ComponentName name)
   {
      Log.d(LOG_TAG, "onServiceDisconnected(): Service has crashed!!");
      mNetService = null;
   }


   public Handler getUiHandler()
   {
      return mUiHandler;
   }

   private void setup()
   {
      mShotClockView = (TextView) findViewById(R.id.shot_clock);
      ActionBar mActionBar = getActionBar();

      if (mActionBar != null) {
         LayoutInflater inflater = (LayoutInflater) getSystemService(Context.LAYOUT_INFLATER_SERVICE);
         @SuppressLint("InflateParams") View scoreBoardView = inflater.inflate(R.layout.score, null);

         mConState = (ImageButton) scoreBoardView.findViewById(R.id.con_state);
         mEnemyShipsView = (TextView) scoreBoardView.findViewById(R.id.enemy_ships);
         mMyShipsView = (TextView) scoreBoardView.findViewById(R.id.my_ships);

         mActionBar.setDisplayShowHomeEnabled(false);
         mActionBar.setDisplayHomeAsUpEnabled(false);
         mActionBar.setDisplayUseLogoEnabled(false);
         mActionBar.setDisplayShowTitleEnabled(false);
         mActionBar.setDisplayShowCustomEnabled(true);

         mActionBar.setCustomView(scoreBoardView);
      }

      mGridButtonHandler = new GridButtonHandler(this);
      bindService(new Intent(this, NetService.class), this, BIND_AUTO_CREATE);
   }

   private void buildGameBoard()
   {
      DisplayMetrics metrics = getResources().getDisplayMetrics();
      ViewGroup enemyFrame = (ViewGroup) findViewById(R.id.enemy_fleet_frame);
      ViewGroup ownFrame = (ViewGroup) findViewById(R.id.own_fleet_frame);

      int minEnemyFrameSize = enemyFrame.getMeasuredWidth() > enemyFrame.getMeasuredHeight() ?
              enemyFrame.getMeasuredHeight() : enemyFrame.getMeasuredWidth();

      int minOwnFrameSize = ownFrame.getMeasuredWidth() > ownFrame.getMeasuredHeight() ?
              ownFrame.getMeasuredHeight() : ownFrame.getMeasuredWidth();

      minEnemyFrameSize = 12 * (int) (minEnemyFrameSize / 12.0f);
      minOwnFrameSize = 12 * (int) (minOwnFrameSize / 12.0f);

      LayoutInflater inflater = LayoutInflater.from(this);

      View mEnemyBoard = inflater.inflate(R.layout.board, enemyFrame, false);
      mEnemyFleetView = new EnemyFleetView(this, (ViewGroup) mEnemyBoard, mGridButtonHandler, minEnemyFrameSize - 12);
      enemyFrame.addView(mEnemyBoard);

      View mOwnBoard = inflater.inflate(R.layout.board, enemyFrame, false);
      mOwnFleetView = new OwnFleetView(this, (ViewGroup) mOwnBoard, minOwnFrameSize - (int) (30 * metrics.density));
      ownFrame.addView(mOwnBoard);

      if (mGameEngine != null) {
         mGameEngine.setModelUpdateListener(mOwnFleetView, mEnemyFleetView);
      }

   }

   private void quitApp()
   {
      if (mNetService != null) {
         mNetService.setListener(null);
      }

      if (mGameEngine != null) {
         mGameEngine.shutDown();
      }
   }
}