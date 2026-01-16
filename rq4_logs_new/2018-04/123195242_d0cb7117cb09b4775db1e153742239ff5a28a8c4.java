package onestep.id.joints;

import android.app.Fragment;
import android.content.Intent;
import android.support.v4.app.FragmentManager;
import android.support.v4.app.FragmentPagerAdapter;
import android.support.v4.view.ViewPager;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

import java.util.List;

import me.relex.circleindicator.CircleIndicator;

public class Index extends AppCompatActivity {
    ViewPager viewPager;
    CircleIndicator circleIndicator;
    Button login, regis;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_index);
        viewPager = (ViewPager) findViewById(R.id.viewPager);
        setupViewPager(viewPager);
        circleIndicator = (CircleIndicator) findViewById(R.id.slide);
        circleIndicator.setViewPager(viewPager);
        regis = (Button) findViewById(R.id.buttonRegis);
        regis.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent i = new Intent(Index.this, Register.class);
                startActivity(i);
            }
        });
        login = (Button) findViewById(R.id.buttonLogin);
        login.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent i = new Intent(Index.this, Login.class);
                startActivity(i);
            }
        });
    }
    private void setupViewPager(ViewPager viewPager) {
        ViewPagerAdapter adapter = new ViewPagerAdapter(getSupportFragmentManager());
        adapter.addFragment(new Index1Fragment());
        adapter.addFragment(new Index2Fragment());
        adapter.addFragment(new Index3Fragment());
        viewPager.setAdapter(adapter);
    }



    class ViewPagerAdapter extends FragmentPagerAdapter {
        private List<android.support.v4.app.Fragment> mFragmentsList = new java.util.ArrayList<>();

        public ViewPagerAdapter(FragmentManager manager) {
            super(manager);
        }

        @Override
        public android.support.v4.app.Fragment getItem(int position) {
            return mFragmentsList.get(position);
        }

        @Override
        public int getCount() {
            return mFragmentsList.size();
        }

        public void addFragment(android.support.v4.app.Fragment fragment) {
            mFragmentsList.add(fragment);
        }
    }
}