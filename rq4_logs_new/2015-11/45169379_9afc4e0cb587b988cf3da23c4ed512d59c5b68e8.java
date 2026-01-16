package yunstudio2015.android.yunmeet.activityz;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.view.ViewPager;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.HorizontalScrollView;

import butterknife.Bind;
import butterknife.ButterKnife;
import yunstudio2015.android.yunmeet.R;
import yunstudio2015.android.yunmeet.customviewz.SlidingTabLayout;
import yunstudio2015.android.yunmeet.tricksbag.VpAdap;

public class ShowActivity extends AppCompatActivity {


    @Nullable @Bind(R.id.toolbar_show)
    Toolbar toolbar;

    // 包括 、广场  、关注的那个菜单
    @Bind(R.id.stl_menu)
    SlidingTabLayout slidingMenu;

    @Bind(R.id.viewpager_container)
    ViewPager vp_container;

    @Bind(R.id.hsc_categories)
    HorizontalScrollView hsc_categories;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_show);
        ButterKnife.bind(this);

        /**
         * 1、 把xml文件里的toolbar设计成当前activity的默认actionbar
         * 2、 把菜单添加上去
         */
        setUpFragments (vp_container);
        setUpCategoriesHv (hsc_categories);
        if (toolbar != null) {
            setSupportActionBar(toolbar);
            setUpSlidingTabs(slidingMenu);
        }
        // 创建一个activity基于两个不同的借口去实现数据及信息展示

    }

    private void setUpCategoriesHv(HorizontalScrollView hsc_categories) {

        // standard categories, and other categories could be got the db
        // we doing this so that in case of no connection, there is
        // still some categories to show if we want to subdivise
        // or get a way to actualize the activity local database everytime it got
        // some changes.
    }

    private void setUpFragments(ViewPager vp_container) {

        VpAdap adap = new VpAdap(getSupportFragmentManager());
        vp_container.setAdapter(adap);
    }

    private void setUpSlidingTabs(SlidingTabLayout tabs) {

        tabs.setDistributeEvenly(true); // To make the Tabs Fixed set this true,
        // This makes the tabs Space Evenly in Available width
        // Setting Custom Color for the Scroll bar indicator of the Tab View
        tabs.setCustomTabColorizer(new SlidingTabLayout.TabColorizer() {
            @Override
            public int getIndicatorColor(int position) {
                return getResources().getColor(R.color.little_red);
            }
        });
        // Setting the ViewPager For the SlidingTabsLayout
        tabs.setViewPager(vp_container);
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_show, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }

        return super.onOptionsItemSelected(item);
    }
}