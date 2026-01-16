package io.happylrd.daphne.adapter;

import android.support.v7.widget.RecyclerView;
import android.view.View;
import android.widget.TextClock;
import android.widget.TextView;

import java.util.List;

import butterknife.BindView;
import io.happylrd.daphne.R;
import io.happylrd.youbo.common.widget.component.FakeModel;
import io.happylrd.youbo.common.widget.recycler.RecyclerAdapter;

/**
 * <h1>modify class name here</h1>
 * io.happylrd.daphne.adapter
 * <p>
 * add description here
 *
 * @author Crepusculo
 * @version 1.0
 * @since 2017/7/6
 */

public class PageRecyclerAdapter<T> extends RecyclerAdapter {
    public PageRecyclerAdapter(List<T> dataset,AdapterListener<T> listener){
        super(dataset,listener);
    }

//    // 进行 view 和 holder 的绑定
//    @Override
//    public void onBindViewHolder(RecyclerView.ViewHolder holder, int position) {
//        // bind
//    }

    // 获取 position 所在位置的布局
    @Override
    protected int getItemViewType(int position, Object data) {
//       return data.type;
        return R.layout.item_card_general;
    }

    // inflate 在这里已经完成, 我们只需要根据 viewType 构建 ViewHolder
    @Override
    protected ViewHolder onCreateViewHolder(View root, int viewType) {
        switch (viewType){
            default:
                return new GeneralViewHolder(root);
        }
    }

    @Override
    public void update(Object data, ViewHolder holder) {

    }

    public class GeneralViewHolder extends ViewHolder<FakeModel>{
        @BindView(R.id.title)
        TextView tTitle;
        @BindView(R.id.author)
        TextView tAuthor;
        @BindView(R.id.date)
        TextClock tClock;

        public GeneralViewHolder(View itemView) {
            super(itemView);
        }
        @Override
        protected void onBind(FakeModel data) {
            this.tTitle.setText(data.getTitle());
            this.tAuthor.setText(data.getTitle());
            this.tClock.setFormat12Hour("h:mm");
        }
    }
}