package com.example.finalproject;

import android.content.Context;
import android.text.format.Time;
import android.util.AttributeSet;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.MotionEvent;
import android.view.View;
import android.view.ViewGroup;
import android.view.animation.RotateAnimation;
import android.widget.AbsListView;
import android.widget.AbsListView.OnScrollListener;
import android.widget.ImageView;
import android.widget.ListView;
import android.widget.ProgressBar;
import android.widget.TextView;

/**
 * Created by 黄健军 on 2016/12/16.
 */

public class RefreshListView extends ListView implements OnScrollListener {

    View header;
    int headerHeight;
    int firstVisibleItem;

    boolean isFlag;
    int startY;

    int state;
    final int NONE = 0;
    final int PULL = 1;
    final int RELEASE = 2;
    final int REFRESH = 3;

    final String TAG = "RefreshListView";

    int scrollState;

    IRefreshListener iRefreshlistener;

    int updateTime;
    int time;
    
    public RefreshListView(Context context) {
        super(context);
        initView(context);
    }
    public RefreshListView(Context context, AttributeSet attrs) {
        super(context, attrs);
        initView(context);
    }
    public RefreshListView(Context context, AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
        initView(context);
    }

    private void initView(Context context){
        
        LayoutInflater inflater = LayoutInflater.from(context);
        header = inflater.inflate(R.layout.rl_header_layout, null);
        measureView(header);
        headerHeight = header.getMeasuredHeight();
        Log.i("headerHeight", ""+headerHeight);
        topPadding(-headerHeight);
        this.addHeaderView(header);
        this.setOnScrollListener(this);

        Time t = new Time();
        t.setToNow();
        updateTime = t.hour*60+t.minute;
        Log.i(TAG, "init--->"+updateTime);
    }

    private void topPadding(int topPadding){

        header.setPadding(header.getPaddingLeft(), topPadding,
                header.getPaddingRight(), header.getPaddingBottom());
        
        header.invalidate();
    }

    private void measureView(View view){
        
        ViewGroup.LayoutParams p = view.getLayoutParams();
        if(p==null){
            
            p = new ViewGroup.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT,
                    ViewGroup.LayoutParams.WRAP_CONTENT);
        }
        
        int width = ViewGroup.getChildMeasureSpec(0, 0, p.width);
        int height;
        int tempHeight = p.height;
        if(tempHeight>0){
            height = MeasureSpec.makeMeasureSpec(tempHeight, MeasureSpec.EXACTLY);
        }else{
            height = MeasureSpec.makeMeasureSpec(0, MeasureSpec.UNSPECIFIED);
        }
        view.measure(width, height);
    }

    @Override
    public void onScrollStateChanged(AbsListView view, int scrollState) {
        
        this.scrollState = scrollState;
    }
    @Override
    public void onScroll(AbsListView view, int firstVisibleItem,
                         int visibleItemCount, int totalItemCount) {
        
        this.firstVisibleItem = firstVisibleItem;
    }
    @Override
    public boolean onTouchEvent(MotionEvent ev) {
        
        switch (ev.getAction()) {
            case MotionEvent.ACTION_DOWN:
                if(firstVisibleItem == 0){
                    isFlag = true;
                    startY = (int)ev.getY();
                }
                break;
            case MotionEvent.ACTION_MOVE:
                onMove(ev);
                break;
            case MotionEvent.ACTION_UP:
                if(state == RELEASE){
                    state = REFRESH;
                    
                    refreshViewByState();
                    iRefreshlistener.onRefresh();
                }else if(state == PULL){
                    state = NONE;
                    isFlag = false;
                    refreshViewByState();
                }
                break;
        }
        return super.onTouchEvent(ev);
    }

    private void onMove(MotionEvent ev){
        
        if(!isFlag){
            return;
        }
        int currentY = (int)ev.getY();
        int space = currentY - startY;
        int topPadding = space - headerHeight;
        switch (state) {
            
            case NONE:
                if(space>0){
                    state = PULL;
                    refreshViewByState();
                }
                break;
            case PULL:
                topPadding(topPadding);
                if(space>headerHeight+30
                        &&scrollState == SCROLL_STATE_TOUCH_SCROLL ){
                    state = RELEASE;
                    refreshViewByState();
                }
                break;
            case RELEASE:
                topPadding(topPadding);
                if(space<headerHeight+30){
                    state = PULL;         
                    refreshViewByState();
                }else if(space<=0){  
                    state = NONE;    
                    isFlag = false;  
                    refreshViewByState();
                }
                break;
        }
    }

    private void refreshViewByState(){
        
        TextView tips = (TextView)header.findViewById(R.id.tips);
        
        ImageView arrow = (ImageView)header.findViewById(R.id.arrow);
        
        ProgressBar progress = (ProgressBar)header.findViewById(R.id.progress);
        
        RotateAnimation animation1 = new RotateAnimation(0, 180,
                RotateAnimation.RELATIVE_TO_SELF,0.5f,
                RotateAnimation.RELATIVE_TO_SELF,0.5f);
        animation1.setDuration(500);
        animation1.setFillAfter(true);
        
        RotateAnimation animation2 = new RotateAnimation(180, 0,
                RotateAnimation.RELATIVE_TO_SELF,0.5f,
                RotateAnimation.RELATIVE_TO_SELF,0.5f);
        animation2.setDuration(500);
        animation2.setFillAfter(true);

        switch (state) {
            case NONE:                     
                arrow.clearAnimation();    
                topPadding(-headerHeight); 
                break;

            case PULL:                                
                arrow.setVisibility(View.VISIBLE);    
                progress.setVisibility(View.GONE);    
                tips.setText("下拉刷新");
                arrow.clearAnimation();               
                arrow.setAnimation(animation2);
                break;

            case RELEASE:                            
                arrow.setVisibility(View.VISIBLE);   
                progress.setVisibility(View.GONE);   
                tips.setText("松开刷新");
                arrow.clearAnimation();              
                arrow.setAnimation(animation1);
                break;

            case REFRESH:                             
                topPadding(50);                       
                arrow.setVisibility(View.GONE);       
                progress.setVisibility(View.VISIBLE); 
                tips.setText("正在刷新");
                arrow.clearAnimation();                
                break;

        }
    }

    public void refreshComplete(){
        state = NONE;   
        isFlag = false;
        refreshViewByState();
        
        Time t = new Time();
        t.setToNow();
        time = t.hour*60+t.minute-updateTime;
        updateTime = t.hour*60+t.minute;
        TextView lastUpdateTime = (TextView)findViewById(R.id.time);
        lastUpdateTime.setText(time+"分钟前更新");
    }
    public void setInterface(IRefreshListener listener){
        this.iRefreshlistener = listener;
    }

    public interface IRefreshListener{
        void onRefresh();
    }
}