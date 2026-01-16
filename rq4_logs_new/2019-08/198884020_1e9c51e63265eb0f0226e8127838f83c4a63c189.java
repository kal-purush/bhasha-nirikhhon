package app.healthyme.com.healthyme;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.view.ViewPager;
import android.support.v7.app.AppCompatActivity;
import android.widget.ImageView;


import app.healthyme.com.healthyme.adapter.MyPagerAdapter;
import app.healthyme.com.healthyme.fragments.IntroSecondFragment;
import app.healthyme.com.healthyme.fragments.IntroThirdFragment;
import app.healthyme.com.healthyme.fragments.intoFirstFragment;

public class WelcomeActivity extends AppCompatActivity implements ViewPager.OnPageChangeListener {
    MyPagerAdapter adapter;
    ImageView firstDotImageView,secondDotImageView,thirdDotImageView;
    ViewPager viewPager;
    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_welcome_intro);

        viewPager=(ViewPager)findViewById(R.id.myViewPager);
        firstDotImageView=(ImageView)findViewById(R.id.thirdDotImageView);
        secondDotImageView=(ImageView)findViewById(R.id.secondDotImageView);
        thirdDotImageView=(ImageView)findViewById(R.id.thirdDotImageView);
        adapter= new MyPagerAdapter(getSupportFragmentManager());

        addFragements();
        viewPager.setAdapter(adapter);
        viewPager.addOnPageChangeListener(this);



    }

    private void addFragements() {
        adapter.addfragments(new intoFirstFragment());
        adapter.addfragments(new IntroSecondFragment());
        adapter.addfragments(new IntroThirdFragment());
    }

    @Override
    public void onPageScrolled(int i, float v, int i1) {

    }

    @Override
    public void onPageSelected(int i) {

        switch (i)
        {
            case 0:
                firstDotImageView.setImageResource(R.drawable.current_position_icon);
                secondDotImageView.setImageResource(R.drawable.disable_position_icon);
                thirdDotImageView.setImageResource(R.drawable.disable_position_icon);

                break;

            case 1:
                firstDotImageView.setImageResource(R.drawable.disable_position_icon);
                secondDotImageView.setImageResource(R.drawable.current_position_icon);
                thirdDotImageView.setImageResource(R.drawable.disable_position_icon);

                break;

            case 2:
                firstDotImageView.setImageResource(R.drawable.disable_position_icon);
                secondDotImageView.setImageResource(R.drawable.disable_position_icon);
                thirdDotImageView.setImageResource(R.drawable.current_position_icon);

                break;
        }

    }

    @Override
    public void onPageScrollStateChanged(int i) {

    }
}