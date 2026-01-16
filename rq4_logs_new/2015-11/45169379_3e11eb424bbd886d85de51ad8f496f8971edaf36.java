package yunstudio2015.android.yunmeet.activityz;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import butterknife.Bind;
import butterknife.ButterKnife;
import yunstudio2015.android.yunmeet.R;

public class TestingShowActivity extends AppCompatActivity {

    @Bind(R.id.my_recycler_view)
    RecyclerView mRecyclerView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_testing_show);
        ButterKnife.bind(this);
        // use this setting to improve performance if you know that changes
        // in content do not change the layout size of the RecyclerView
        mRecyclerView.setHasFixedSize(true);

        // use a linear layout manager
        LinearLayoutManager mLayoutManager = new LinearLayoutManager(this);
        mRecyclerView.setLayoutManager(mLayoutManager);

        // specify an adapter (see also next example)
        String[] myDataset = {"无聊", "文化", "电视"};
        MyAdapter mAdapter = new MyAdapter(myDataset);
        mRecyclerView.setAdapter(mAdapter);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_testing_show, menu);
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

      class MyAdapter extends RecyclerView.Adapter<MyAdapter.ViewHolder> {
        private String[] mDataset;

        // Provide a reference to the views for each data item
        // Complex data items may need more than one view per item, and
        // you provide access to all the views for a data item in a view holder
        public class ViewHolder extends RecyclerView.ViewHolder {
            // each data item is just a string in this case
            public TextView mTextView;
            public ViewHolder(View v) {
                super(v);
//                mTextView = v;
            }
        }

        // Provide a suitable constructor (depends on the kind of dataset)
        public MyAdapter(String[] myDataset) {
            mDataset = myDataset;
        }

        // Create new views (invoked by the layout manager)
        @Override
        public MyAdapter.ViewHolder onCreateViewHolder(ViewGroup parent,
                                                       int viewType) {
            // create a new view
//            View v = LayoutInflater.from(parent.getContext())
//                    .inflate(R.layout.my_text_view, parent, false);
//            // set the view's size, margins, paddings and layout parameters
            View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.activity_item_xml, parent, false);
            ViewHolder vh = new ViewHolder(view);
            return vh;
        }

          /*    private void addFakeElement(LayoutInflater inflater, LinearLayout lny_container) {

        View view = inflater.inflate(R.layout.activity_item_xml, lny_container, false);
        RelativeLayout relative = ButterKnife.findById(view, R.id.relative);
        lny_container.addView(view);
        ImageView iv = (ImageView) view.findViewById(R.id.iv_activity_bg);
//        Bitmap bm = ((BitmapDrawable) iv.getDrawable()).getBitmap();
//        iv.setImageDrawable(new RoundedDrawable(bm, bm.getScaledWidth(metrics)));
        // set up the width of the iv
        RelativeLayout.LayoutParams layoutParams = (RelativeLayout.LayoutParams) iv.getLayoutParams();
//        layoutParams.height = layoutParams.width;
        iv.setLayoutParams(layoutParams);
        String link = "http://www.csw333.com/upload_files/qibosoft_news_/135/83555_20141120091105_8uctj.jpg";//"http://p5.img.cctvpic.com/nettv/newgame/2011/1118/20111118105936548.jpg";
        ImageLoader.getInstance().displayImage(link, iv, ImageLoadOptions.getDisplaySlightlyRoundedImageOptions(getContext()));
    }*/

          // Replace the contents of a view (invoked by the layout manager)
        @Override
        public void onBindViewHolder(ViewHolder holder, int position) {
            // - get element from your dataset at this position
            // - replace the contents of the view with that element
//            holder.mTextView.setText(mDataset[position]);

        }

        // Return the size of your dataset (invoked by the layout manager)
        @Override
        public int getItemCount() {
            return mDataset.length;
        }
    }

}