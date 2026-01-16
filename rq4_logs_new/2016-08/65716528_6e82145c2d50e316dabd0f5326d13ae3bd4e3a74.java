package pf.xqq.adapter;

import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;

import java.util.ArrayList;
import java.util.List;

import pf.xqq.Main;
import pf.xqq.R;


/**
 * Created by jianghejie on 15/11/26.
 */
public class AdapterMain extends RecyclerView.Adapter<AdapterMain.ViewHolder> {

    private List<String> datas1 = new ArrayList<String>();
    private Main mAct1;


    public AdapterMain(List<String> datas, Main act) {
        datas1 = datas;
        mAct1 = act;
    }

    //创建新View，被LayoutManager所调用
    @Override
    public ViewHolder onCreateViewHolder(ViewGroup viewGroup, int viewType) {
        View view = LayoutInflater.from(viewGroup.getContext()).inflate(R.layout.main_item, viewGroup, false);
        return new ViewHolder(view);
    }

    //将数据与界面进行绑定的操作
    @Override
    public void onBindViewHolder(ViewHolder viewHolder, final int position) {
        String str = datas1.get(position);
        viewHolder.btn.setText(str == null ? "" : str);
        viewHolder.btn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mAct1.onClickItem(datas1.get(position));
            }
        });
    }

    //获取数据的数量
    @Override
    public int getItemCount() {
        return datas1.size();
    }

    //自定义的ViewHolder，持有每个Item的的所有界面元素
    public static class ViewHolder extends RecyclerView.ViewHolder {
        public Button btn;

        public ViewHolder(View view) {
            super(view);
            btn = (Button) itemView.findViewById(R.id.main_item_btn);
        }
    }


}