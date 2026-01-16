package br.edu.ifce.engcomp.francis.diversidade.activities;

import android.app.Fragment;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;

import br.edu.ifce.engcomp.francis.diversidade.R;
import br.edu.ifce.engcomp.francis.diversidade.fragments.NucleusFragment;

/**
 * Created by Bolsista on 04/04/2016.
 */
public class SplashFragment extends AppCompatActivity {
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_splash);

        if (savedInstanceState == null) {
            Fragment fragmentNucleus = new NucleusFragment();
            android.app.FragmentManager fragmentManagerNucleus = getFragmentManager();
            fragmentManagerNucleus.beginTransaction().replace(R.id.content_splash, fragmentNucleus).commit();
        }
    }
}
