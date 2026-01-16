package protocounter.sbl.hanafudacounter;

import android.content.pm.ActivityInfo;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.CompoundButton;
import android.widget.Switch;
import android.widget.TextView;
import android.widget.ToggleButton;

import org.w3c.dom.Text;

public class MainActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        setRequestedOrientation(ActivityInfo.SCREEN_ORIENTATION_PORTRAIT);


    }


// We declare and initialize an variable in order to show the state of the switch used to select the player

    boolean playerSelection = false;


// we define a function in order to check the state of the switch (false for player 1, true for player 2)

    public void checkSwitch() {
        Switch playerSwitch = (Switch) findViewById(R.id.playerSelect);
        playerSelection = playerSwitch.isChecked();
    }

    boolean koiKoiState = false;

    public void checkKoiKoi() {
        ToggleButton koiKoiToggle = (ToggleButton) findViewById(R.id.koiKoiButton);
        koiKoiState = koiKoiToggle.isChecked();
    }

    // we declare and initialize a boolean variable for each combination possible


    boolean goukouState = false;
    boolean shikouState = false;
    boolean ameShikouState = false;
    boolean sankouState = false;
    boolean inoshikachouState = false;
    boolean akatanState = false;
    boolean aotanState = false;
    boolean taneState = false;
    boolean tanState = false;
    boolean kasuState = false;
    boolean tsukimiState = false;
    boolean hanamiState = false;
    int taneBonus = 0;
    int tanBonus = 0;
    int kasuBonus = 0;

    // We code a function to check the state of each button

    public void checkGoukou() {
        ToggleButton goukouButton = (ToggleButton) findViewById(R.id.goukou);
        goukouState = goukouButton.isChecked();
    }

    public void checkShikou() {
        ToggleButton shikouButton = (ToggleButton) findViewById(R.id.shikou);
        shikouState = shikouButton.isChecked();
    }

    public void checkAmeShikou() {
        ToggleButton ameShikouButton = (ToggleButton) findViewById(R.id.ameShikou);
        ameShikouState = ameShikouButton.isChecked();
    }

    public void checkSankou() {
        ToggleButton sankouButton = (ToggleButton) findViewById(R.id.sankou);
        sankouState = sankouButton.isChecked();
    }

    public void checkInoshikachou() {
        ToggleButton inoshikachouButton = (ToggleButton) findViewById(R.id.inoshikachou);
        inoshikachouState = inoshikachouButton.isChecked();
    }

    public void checkAkatan() {
        ToggleButton akatanButton = (ToggleButton) findViewById(R.id.akatan);
        akatanState = akatanButton.isChecked();
    }

    public void checkAotan() {
        ToggleButton aotanButton = (ToggleButton) findViewById(R.id.aotan);
        aotanState = aotanButton.isChecked();
    }

    public void checkTane() {
        ToggleButton taneButton = (ToggleButton) findViewById(R.id.tane);
        taneState = taneButton.isChecked();
    }

    public void checkTan() {
        ToggleButton tanButton = (ToggleButton) findViewById(R.id.tan);
        tanState = tanButton.isChecked();
    }

    public void checkKasu() {
        ToggleButton kasuButton = (ToggleButton) findViewById(R.id.kasu);
        kasuState = kasuButton.isChecked();
    }

    public void checkTsukimi() {
        ToggleButton tsukimiButton = (ToggleButton) findViewById(R.id.tsukimi);
        tsukimiState = tsukimiButton.isChecked();
    }

    public void checkHanami() {
        ToggleButton hanamiButton = (ToggleButton) findViewById(R.id.hanami);
        hanamiState = hanamiButton.isChecked();
    }

    // We code the function to increase or decrease bonus points

    public void displayTane() {
        TextView taneBonusView = (TextView) findViewById(R.id.taneBonusText);
        taneBonusView.setText(String.valueOf(taneBonus));
    }

    public void plusTane(View view) {
        taneBonus += 1;
        displayTane();
    }

    public void minusTane(View view) {
        taneBonus -= 1;
        displayTane();
    }

    public void displayTan() {
        TextView tanBonusView = (TextView) findViewById(R.id.tanBonusText);
        tanBonusView.setText(String.valueOf(tanBonus));
    }

    public void plusTan(View view) {
        tanBonus += 1;
        displayTan();
    }

    public void minusTan(View view) {
        tanBonus -= 1;
        displayTan();
    }

    public void displayKasu() {
        TextView kasuBonusView = (TextView) findViewById(R.id.kasuBonusText);
        kasuBonusView.setText(String.valueOf(kasuBonus));
    }

    public void plusKasu(View view) {
        kasuBonus += 1;
        displayKasu();
    }

    public void minusKasu(View view) {
        kasuBonus -= 1;
        displayKasu();
    }

    int scoreDisplayed = 0;
    // The following function will check the state of all the buttons. It will be called when clicking the total button

    public void checkAllButtons() {
        checkKoiKoi();
        checkAkatan();
        checkAmeShikou();
        checkAotan();
        checkGoukou();
        checkHanami();
        checkInoshikachou();
        checkKasu();
        checkSankou();
        checkShikou();
        checkTan();
        checkTane();
        checkTsukimi();

        scoreDisplayed = calculateScore();

    }


    public void totalButton(View view) {

        checkAllButtons();
        TextView scoreDisplay = (TextView) findViewById(R.id.scoreDisplay);
        scoreDisplay.setText(String.valueOf(String.valueOf(scoreDisplayed)));

    }


// The following function will be calculating the score a player earned at the end of a set

    public int calculateScore() {

        int totalScore = 0;

        if (goukouState == true) {
            totalScore += 10;
        }

        if (shikouState == true) {
            totalScore += 8;
        }

        if (ameShikouState == true) {
            totalScore += 7;
        }

        if (sankouState == true) {
            totalScore += 5;
        }

        if (inoshikachouState == true) {
            totalScore += 5;
        }

        // There is a special rule for akatan and aotan, so we need to use cumultaive conditions here

        if ((akatanState == true) && (aotanState == false)) {

            totalScore += 5;
        }

        if ((akatanState == false) && (aotanState == true)) {

            totalScore += 5;
        }

        if ((akatanState == true) && (aotanState == true)) {

            totalScore += 10;
        }

        if (tsukimiState == true) {
            totalScore += 5;
        }

        if (hanamiState == true) {
            totalScore += 5;
        }


        // For the combination that includes bonus score, we add the bonus core plus the point for the combination
        if (taneState == true) {
            totalScore += 1;
            totalScore += taneBonus;
        }
        if (tanState == true) {
            totalScore += 1;
            totalScore += tanBonus;
        }

        if (kasuState == true) {
            totalScore += 1;
            totalScore += kasuBonus;
        }

        // if Koi Koi button is checked, points are doubled
        if (koiKoiState == true) {

            totalScore = totalScore * 2;
        }

        // At the end of the function, the total score is returned
        return totalScore;
    }

    public void unCheckButtons() {
        ToggleButton goukou = (ToggleButton) findViewById(R.id.goukou);
        goukou.setChecked(false);
        ToggleButton shikou = (ToggleButton) findViewById(R.id.shikou);
        shikou.setChecked(false);
        ToggleButton ameshikou = (ToggleButton) findViewById(R.id.ameShikou);
        ameshikou.setChecked(false);
        ToggleButton sankou = (ToggleButton) findViewById(R.id.sankou);
        sankou.setChecked(false);
        ToggleButton inoshikachou = (ToggleButton) findViewById(R.id.inoshikachou);
        inoshikachou.setChecked(false);
        ToggleButton akatan = (ToggleButton) findViewById(R.id.akatan);
        akatan.setChecked(false);
        ToggleButton aotan = (ToggleButton) findViewById(R.id.aotan);
        aotan.setChecked(false);
        ToggleButton tane = (ToggleButton) findViewById(R.id.tane);
        tane.setChecked(false);
        ToggleButton tan = (ToggleButton) findViewById(R.id.tan);
        tan.setChecked(false);
        ToggleButton kasu = (ToggleButton) findViewById(R.id.kasu);
        kasu.setChecked(false);
        ToggleButton tsukimi = (ToggleButton) findViewById(R.id.tsukimi);
        tsukimi.setChecked(false);
        ToggleButton hanami = (ToggleButton) findViewById(R.id.hanami);
        hanami.setChecked(false);

        ToggleButton koikoi = (ToggleButton) findViewById(R.id.koiKoiButton);
        koikoi.setChecked(false);


        tanBonus = 0;
        displayTan();
        taneBonus = 0;
        displayTane();
        kasuBonus = 0;
        displayKasu();
        scoreDisplayed = 0;
        TextView scoreDisplay = (TextView) findViewById(R.id.scoreDisplay);
        scoreDisplay.setText(String.valueOf(String.valueOf(scoreDisplayed)));
    }

    // at last, a function adding the score calculated to the total of player 1 or 2 regarding to the switch state
    int player1score = 0;
    int player2score = 0;

    public void scorePoints(View view) {
        TextView score1 = (TextView) findViewById(R.id.player1_score);
        TextView score2 = (TextView) findViewById(R.id.player2_score);

        checkSwitch();

        if (playerSelection == false) {
            player1score += scoreDisplayed;
            score1.setText(String.valueOf(player1score));
        }

        if (playerSelection == true) {
            player2score += scoreDisplayed;
            score2.setText(String.valueOf(player2score));
        }

        unCheckButtons();

    }

    public void resetScores(View view) {

        player1score = 0;
        player2score = 0;
        TextView score1 = (TextView) findViewById(R.id.player1_score);
        TextView score2 = (TextView) findViewById(R.id.player2_score);
        score1.setText(String.valueOf(player1score));
        score2.setText(String.valueOf(player2score));
    }

}



