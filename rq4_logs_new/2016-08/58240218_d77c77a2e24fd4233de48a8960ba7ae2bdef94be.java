package edu.csumb.garc4464.colore.colore.OptionsMenu;

import android.app.AlertDialog;
import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;

import butterknife.ButterKnife;
import butterknife.OnClick;
import edu.csumb.garc4464.colore.R;

/**
 * Created by ben on 8/7/16.
 */
public class HelpDialog extends AlertDialog {

    public HelpDialog(Context context) {
        super(context);
        LayoutInflater inflater = LayoutInflater.from(context);
        View view = inflater.inflate(R.layout.help_dialog, null);
        setView(view);
        ButterKnife.bind(this, view);
    }

    @OnClick(R.id.close)
    public void onCloseButtonClicked() {
        hide();
    }
}