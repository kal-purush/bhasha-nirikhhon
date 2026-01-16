
/*
* Copyright 2016 Harsh Patel
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package hp.harsh.wearablefragmentgridpagerview.activity;

import android.app.Activity;
import android.os.Bundle;
import android.support.wearable.view.DotsPageIndicator;
import android.support.wearable.view.GridViewPager;
import android.support.wearable.view.WatchViewStub;

import hp.harsh.wearablefragmentgridpagerview.R;
import hp.harsh.wearablefragmentgridpagerview.adapter.SuperHumanFragmentAdapter;

public class MainActivity extends Activity {

    private GridViewPager mGridPager;

    // used to show indicator for total item in one row
    private DotsPageIndicator mPagerIndicator;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        final WatchViewStub stub = (WatchViewStub) findViewById(R.id.watch_view_stub);

        stub.setOnLayoutInflatedListener(new WatchViewStub.OnLayoutInflatedListener() {
            @Override
            public void onLayoutInflated(WatchViewStub stub) {
                mGridPager = (GridViewPager) stub.findViewById(R.id.gridPager);
                mPagerIndicator = (DotsPageIndicator) stub.findViewById(R.id.pagerIndicator);

                // Set adapter for gridview
                mGridPager.setAdapter(new SuperHumanFragmentAdapter(MainActivity.this, getFragmentManager()));
                mPagerIndicator.setPager(mGridPager);
            }
        });
    }
}