package com.example.pacmanlike.levelmakerstages;

import android.view.View;
import android.widget.Button;
import android.widget.HorizontalScrollView;
import android.widget.ImageButton;
import android.widget.TextView;

import com.example.pacmanlike.R;
import com.example.pacmanlike.activities.LevelMakerActivity;
import com.example.pacmanlike.main.AppConstants;
import com.example.pacmanlike.view.ImportExportMap;
import com.example.pacmanlike.view.MapSquare;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Stage 6 of the level making process.
 * User chooses a name for his map.
 */
public class ChooseNameStage implements StageInterface {
    public final LevelMakerActivity _activity;

    private final HorizontalScrollView _oneTimeButtons;
    private TextView _instructions, _mapName;

    public ChooseNameStage(LevelMakerActivity activity){
        _activity = activity;

        _instructions = (TextView) _activity.findViewById(R.id.instructionView);
        _oneTimeButtons = (HorizontalScrollView) _activity.findViewById(R.id.oneTimeButtonsScrollView);
        _mapName = (TextView) _activity.findViewById(R.id.map_name);
    }

    @Override
    public void onClickMap(MapSquare square, ImageButton button, int selected) throws Exception {
        // Should do nothing during this stage
    }

    /**
     * Sets everything for the next stage:
     * End stage
     * If level name has a non-alphanumeric character,
     * write warning and keep the stage
     * @param continueButton Continue Button
     * @param ieButton Import-Export Button
     * @return Next stage
     */
    @Override
    public LevelMakerActivity.StageEnum nextStageClick(Button continueButton, Button ieButton) throws Exception {
        _activity.levelName = _mapName.getText().toString();
        if (!_activity.levelName.matches("[A-Za-z0-9]+")){
            _instructions.setText(R.string.instructions_invalid_map_name);
            return LevelMakerActivity.StageEnum.CHOOSE_NAME;
        }
        saveMap(_activity.levelName, _activity.levelString);
        _instructions.setText(R.string.instructions_saved);
        continueButton.setText(R.string.return_button);
        _mapName.setVisibility(View.INVISIBLE);
        ieButton.setVisibility(View.INVISIBLE);

        return LevelMakerActivity.StageEnum.END;
    }

    /**
     * Saves the file to the internal storage.
     * @param levelName Name of the file
     * @param levelString Contents of the file
     * @throws IOException
     */
    private void saveMap(String levelName, String levelString) throws IOException {
        File parent = _activity.getApplicationContext().getFilesDir();

        File file = new File(parent, levelName + AppConstants.CSV_EXTENSION);
        FileWriter writer = new FileWriter(file);
        writer.write(levelString);
        writer.flush();
        writer.close();
    }


    /**
     * Called during map export, shows the map code to the user
     * and copies it to clipboard.
     * @param continueButton Continue Button
     * @param ieButton Import-Export Button
     * @param textView Multiline textView from importing/exporting the map
     * @return Next stage
     */
    @Override
    public LevelMakerActivity.StageEnum onImportExportClick(Button continueButton, Button ieButton, TextView textView) {
        String exportString = ImportExportMap.exportMap(_activity.levelString);
        ImportExportMap.copyToClipboard(_activity, exportString);
        _instructions.setText(R.string.clipboard_copied);

        textView.setVisibility(View.VISIBLE);
        textView.setText(exportString);
        ieButton.setVisibility(View.INVISIBLE);

        // Stay on the same stage
        return LevelMakerActivity.StageEnum.CHOOSE_NAME;
    }

}