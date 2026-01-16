package de.mmrsic.mini2dx.taxi.android;

import org.mini2Dx.android.AndroidMini2DxConfig;

import com.badlogic.gdx.backends.android.AndroidMini2DxGame;

import android.os.Bundle;
import de.mmrsic.mini2dx.taxi.TaxiGame;

public class AndroidLauncher extends AndroidMini2DxGame {
  @Override
  protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    AndroidMini2DxConfig config = new AndroidMini2DxConfig(TaxiGame.GAME_IDENTIFIER);
    initialize(new TaxiGame(), config);
  }
}