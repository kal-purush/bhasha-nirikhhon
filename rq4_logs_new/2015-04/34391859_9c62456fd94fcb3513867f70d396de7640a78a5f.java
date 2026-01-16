package com.example.OptimusxAct.Contact;

import android.app.Activity;
import android.app.AlertDialog;
import android.app.Fragment;
import android.content.DialogInterface;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.support.v7.widget.DefaultItemAnimator;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.View.OnClickListener;
import android.view.ViewGroup;
import android.widget.RelativeLayout;
import android.widget.TextView;

import com.example.OptimusxAct.R;
import com.example.android.bluetoothlegatt.IDcard;
import com.optimusx.contacts.ContactsDatabaseHelper;
import com.optimusx.model.MyItemClickListener;

public class AllBusinessCardFragment extends Fragment {

	private LinearLayoutManager mLayoutManager;
	private AllBusinessCardAdapter mAdapter;
	private ContactsDatabaseHelper cdHelper;
	private RecyclerView mRecyclerView;
	private View rootView;

	@Override
	public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        rootView = inflater.inflate(R.layout.activity_all_business_card, container, false);
		mRecyclerView = (RecyclerView) rootView.findViewById(R.id.act_all_busi_card_rv);
		
		//deal with ContactsDatabaseHelper
		cdHelper = new ContactsDatabaseHelper(getActivity().getApplicationContext(), "Contacts.db", null, 3);

		// use a linear layout manager
        mRecyclerView.setLayoutManager(new LinearLayoutManager(getActivity().getApplicationContext()));
        mRecyclerView.setHasFixedSize(true);
        mRecyclerView.setItemAnimator(new DefaultItemAnimator());
        // specify an adapter (see also next example)
        mAdapter = new AllBusinessCardAdapter();
        mAdapter.setOnItemClickListener(new MyItemClickListener() {
			@Override
			public void onItemClick(View view, int position) {
				Intent intent = new Intent(getActivity().getApplicationContext(), ViewContactActivity.class);
				intent.putExtra("position", position);
				startActivity(intent);
			}
		});
        
        mRecyclerView.setAdapter(mAdapter);
		return rootView;
	}
	
	public class AllBusinessCardAdapter extends RecyclerView.Adapter<AllBusinessCardAdapter.ViewHolder> {

	    // Provide a reference to the type of views that you are using
	    // (custom viewholder)
	    public class ViewHolder extends RecyclerView.ViewHolder implements OnClickListener{
	        public View mRootView;
	        private MyItemClickListener mListener;
			
	        public ViewHolder(View rootView, MyItemClickListener listener) {
	            super(rootView);
	            mRootView = rootView;
	            this.mListener = listener;
	            rootView.setOnClickListener(this);
	        }
			@SuppressWarnings("deprecation")
			@Override
			public void onClick(View v) {
				if(mListener != null){
		            mListener.onItemClick(v,getPosition());  
		        }
			}
	    }
		private MyItemClickListener mItemClickListener;
		protected IDcard idc;
		
	    public void setOnItemClickListener(MyItemClickListener listener){  
	        this.mItemClickListener = listener;
	    }
	    
	    // Provide a suitable constructor (depends on the kind of dataset)
	    public AllBusinessCardAdapter() {
	    	
	    }

	    // Create new views (invoked by the layout manager)
	    @Override
	    public AllBusinessCardAdapter.ViewHolder onCreateViewHolder(ViewGroup parent,
	                                                   int viewType) {
	        // create a new view
	        View v = LayoutInflater.from(parent.getContext()).inflate(R.layout.contact, parent, false);
	        ViewHolder vh = new ViewHolder(v, mItemClickListener);
	        return vh;
	    }

	    // Replace the contents of a view (invoked by the layout manager)
	    @Override
	    public void onBindViewHolder(ViewHolder holder, int position) {
            ((TextView)holder.mRootView.findViewById(R.id.contact_name)).setText(cdHelper.getContactName(cdHelper.getWritableDatabase(), position));
            ((TextView)holder.mRootView.findViewById(R.id.contact_phonenum)).setText(cdHelper.getContactPhoneNumber(cdHelper.getWritableDatabase(), position));
            ((TextView)holder.mRootView.findViewById(R.id.contact_schnum)).setText(cdHelper.getContactSchoolNumber(cdHelper.getWritableDatabase(), position));

            //set click listener of remove contact
	        (holder.mRootView.findViewById(R.id.remove_contact)).setOnClickListener(new MyCardViewImgBtnOnClickListener(position) {
                @Override
                public void onClick(View v) {
                    RelativeLayout rl = (RelativeLayout) v.getParent();
                    idc = new IDcard();
                    idc.setName(((TextView)rl.findViewById(R.id.contact_name)).getText().toString());
                    Activity activity = getActivity();
                    while (activity.getParent() != null) {
                        activity = activity.getParent();
                    }
                    new AlertDialog.Builder(activity).setTitle("删除名片")//设置对话框标题
                            .setMessage("真的要删除 " + idc.n + " 吗?")//设置显示的内容
                            .setPositiveButton("确定", new DialogInterface.OnClickListener() {//添加确定按钮
                                @Override
                                public void onClick(DialogInterface dialog, int which) {//确定按钮的响应事件
                                    cdHelper.deleteContact(cdHelper.getWritableDatabase(), idc);
                                    new Handler().post(new Runnable() {
                                        public void run() {
                                            notifyItemRemoved(mPos);
                                            mRecyclerView.postInvalidate();
                                        }
                                    });
                                }
                            }).setNegativeButton("返回",new DialogInterface.OnClickListener() {//添加返回按钮
                        @Override
                        public void onClick(DialogInterface dialog, int which) {//响应事件
                            Log.i("alertdialog"," 请保存数据！");
                        }
                    }).show();//在按键响应事件中显示此对话框
                }
            });

            //set click listener of action call
            (holder.mRootView.findViewById(R.id.action_call)).setOnClickListener(new MyCardViewImgBtnOnClickListener(position) {
                @Override
                public void onClick(View v) {
                    Intent intent = new Intent();
                    intent.setAction(Intent.ACTION_DIAL);
                    intent.setData(Uri.parse("tel:" + cdHelper.getContactPhoneNumber(cdHelper.getWritableDatabase(), mPos)));
                    startActivity(intent);
                }
            });

            //set click listener of action smsto
            (holder.mRootView.findViewById(R.id.action_smsto)).setOnClickListener(new MyCardViewImgBtnOnClickListener(position) {
                @Override
                public void onClick(View v) {
                    Intent intent = new Intent(Intent.ACTION_SENDTO, Uri.parse("smsto://" + cdHelper.getContactPhoneNumber(cdHelper.getWritableDatabase(), mPos)));
                    startActivity(intent);
                }
            });
	    }

        //the ClickListener of image button
	    abstract class MyCardViewImgBtnOnClickListener implements OnClickListener{
            int mPos = 0;
            public MyCardViewImgBtnOnClickListener(int position) {
                mPos = position;
            }
            @Override
            public abstract void onClick(View v);
        }

	    // Return the size of your dataset (invoked by the layout manager)
	    @Override
	    public int getItemCount() {
	    	return cdHelper.getContactCount(cdHelper.getWritableDatabase());
	    }
	}

    public void refreshRecyclerView(){
        mRecyclerView.getAdapter().notifyDataSetChanged();
        mRecyclerView.postInvalidate();
    }
}