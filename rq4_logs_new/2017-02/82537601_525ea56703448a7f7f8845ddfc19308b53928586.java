package com.example.phuong.viectimnguoiapp.adapters;

import android.content.Context;
import android.content.SharedPreferences;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.example.phuong.viectimnguoiapp.R;
import com.example.phuong.viectimnguoiapp.objects.NewItem;
import com.example.phuong.viectimnguoiapp.utils.Constant;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by phuong on 21/02/2017.
 */

public class NewsAdapter extends RecyclerView.Adapter<NewsAdapter.NewsHolder> {
    private List<NewItem> mNews;
    private Context mContext;
    //Firebase mFirebase1, mFirebase2;
    SharedPreferences mSharedPreferencesLogin;

    public NewsAdapter(List<NewItem> mNews, Context mContext) {
        this.mNews = mNews;
        this.mContext = mContext;
        //Firebase.setAndroidContext(mContext);
        mSharedPreferencesLogin = mContext.getSharedPreferences(Constant.DATA_NAME_USER_LOGIN,0);
    }

    @Override
    public NewsHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.news_item, parent, false);
        return new NewsHolder(view);
    }

    @Override
    public void onBindViewHolder(NewsHolder holder, int position) {
        final NewItem item = mNews.get(position);
        holder.mTvDate.setText(item.getDate().toString());
        holder.mTvTitle.setText(item.getTitle());
        holder.mTvDetail.setText(item.getDetail());

        holder.mImgContact.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                String idUser = mSharedPreferencesLogin.getString(Constant.ID_USER_LOGIN,"");
                String username = mSharedPreferencesLogin.getString(Constant.NAME_USER_LOGIN,"");
                if("".equals(idUser)){
                    //show dialog
                }
                else{
                   /* mFirebase1 = new Firebase("https://demochat-b0c7d.firebaseio.com/messages/" + idUser + "_" + item.getIdUser());
                    mFirebase2 = new Firebase("https://demochat-b0c7d.firebaseio.com/messages/" + item.getIdUser() + "_" + idUser);
                    String messageText = username+" đăng ký làm việc "+item.getTitle()+". Bài được đăng lúc "+item.getDate();
                    Map<String, String> map = new HashMap<String, String>();
                    map.put("message", messageText);
                    map.put("user", idUser);
                    mFirebase1.push().setValue(map);
                    mFirebase2.push().setValue(map);*/
                }
            }
        });
    }

    @Override
    public int getItemCount() {
        return (mNews == null) ? 0 : mNews.size();
    }

    public class NewsHolder extends RecyclerView.ViewHolder {

        TextView mTvDate;
        TextView mTvTitle;
        TextView mTvDetail;
        ImageView mImgContact;

        public NewsHolder(View itemView) {
            super(itemView);
            mTvDate = (TextView) itemView.findViewById(R.id.tvDateNew);
            mTvTitle = (TextView) itemView.findViewById(R.id.tvTitleNew);
            mTvDetail = (TextView) itemView.findViewById(R.id.tvDetailNew);
            mImgContact = (ImageView) itemView.findViewById(R.id.imgNewContact);
        }
    }
}