package br.edu.ifce.engcomp.francis.diversidade.activities;

import android.os.Bundle;
import android.support.design.widget.FloatingActionButton;
import android.support.design.widget.Snackbar;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.EditText;
import android.widget.Toast;

import br.edu.ifce.engcomp.francis.diversidade.R;

public class CommentActivity extends AppCompatActivity {
    EditText commentEditText;
    EditText userCommentEditText;
    Toolbar toolbar;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_comment);
        toolbar = (Toolbar) findViewById(R.id.toolbar);
        if (toolbar != null) {
            setSupportActionBar(toolbar);
            getSupportActionBar().setDisplayHomeAsUpEnabled(true);
            getSupportActionBar().setDisplayShowHomeEnabled(true);
        }

        commentEditText = (EditText) findViewById(R.id.new_comment_edit_text);
        userCommentEditText = (EditText) findViewById(R.id.user_comment_edit_text);
        commentEditText.addTextChangedListener(this.buildTextWatcher());
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_comment, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        int id = item.getItemId();

        if (id == R.id.review_menu) {
            //sendReviewToWS();
            Toast.makeText(getBaseContext(), "Comentário enviado!", Toast.LENGTH_SHORT).show();
            return true;
        }

        return super.onOptionsItemSelected(item);
    }

    private TextWatcher buildTextWatcher() {
        return new TextWatcher() {

            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {

            }

            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {
                String subtitle;
                int residualChars = 250 - s.length();

                if (residualChars > 1)
                    subtitle = String.format("%d ", residualChars) + getString(R.string.readers_review_toolbar_subtitle_plural);
                else
                    subtitle = String.format("%d ", residualChars) + getString(R.string.readers_review_toolbar_subtitle);

                toolbar.setSubtitle(subtitle);
            }

            @Override
            public void afterTextChanged(Editable s) {

            }

        };
    }

}