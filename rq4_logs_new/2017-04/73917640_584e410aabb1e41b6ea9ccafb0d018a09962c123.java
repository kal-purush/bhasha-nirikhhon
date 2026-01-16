package com.nju.urbangreen.zhenjiangurbangreen.basisClass;

import android.content.Context;
import android.support.v7.widget.RecyclerView;
import android.text.TextUtils;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Filter;
import android.widget.Filterable;

import com.nju.urbangreen.zhenjiangurbangreen.events.OneEvent;
import com.nju.urbangreen.zhenjiangurbangreen.util.MyApplication;
import com.nju.urbangreen.zhenjiangurbangreen.util.UrbanGreenDB;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Liwei on 2017/4/12.
 */
public abstract class BaseListAdapter extends RecyclerView.Adapter<BaseRecordViewHolder> implements Filterable{

    public static final int ITEM_TYPE_CONTENT = 0;
    public static final int ITEM_TYPE_FOOT = 1;

    protected List<OneEvent> mData;
    protected List<OneEvent> mFilterData;
    protected final Context mContext;
    protected LayoutInflater mInflater;

    private BaseRecordFilter filter;
    private OnItemClickListener mClickListener;
    private OnItemLongClickListener mLongClickListener;

    private int position;//用来记录事件活动当前处在哪个fragment上

    public BaseListAdapter(Context context, List<OneEvent> list, int position){
        mContext = context;
        mData = list;
        mInflater = LayoutInflater.from(context);
        this.position = position;
    }
    @Override
    public BaseRecordViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        final BaseRecordViewHolder holder = new BaseRecordViewHolder(mContext,
                mInflater.inflate(getItemLayoutId(viewType), parent, false));
        if (mClickListener != null) {
            holder.itemView.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    mClickListener.onItemClick(holder.itemView, holder.getLayoutPosition());
                }
            });
        }
        if (mLongClickListener != null) {
            holder.itemView.setOnLongClickListener(new View.OnLongClickListener() {
                @Override
                public boolean onLongClick(View v) {
                    mLongClickListener.onItemLongClick(holder.itemView, holder.getLayoutPosition());
                    return true;
                }
            });
        }
        return holder;

    }

    @Override
    public void onBindViewHolder(BaseRecordViewHolder holder, int position) {
        bindData(holder, position, mData.get(position));
    }

    @Override
    public int getItemCount() {
        return mData.size();
    }

    public void add(int pos, OneEvent item) {
        mData.add(pos, item);
        notifyItemInserted(pos);
    }

    public void delete(int pos) {
        mData.remove(pos);
        notifyItemRemoved(pos);
    }

    public void setOnItemClickListener(OnItemClickListener listener) {
        mClickListener = listener;
    }

    public void setOnItemLongClickListener(OnItemLongClickListener listener) {
        mLongClickListener = listener;
    }

    abstract public int getItemLayoutId(int viewType);

    abstract public void bindData(BaseRecordViewHolder holder, int position, OneEvent item);

    public interface OnItemClickListener {
        public void onItemClick(View itemView, int pos);
    }

    public interface OnItemLongClickListener {
        public void onItemLongClick(View itemView, int pos);
    }

    @Override
    public Filter getFilter() {
        if(this.filter == null){
            this.filter = new BaseRecordFilter(this,mData);
        }
        return this.filter;
    }

    public void updateDataFromDB()
    {
        //从数据库查询得到最新的事件列表
        List<OneEvent> updateList=new ArrayList<>(UrbanGreenDB.getInstance(MyApplication.getContext())
                .loadEventsWithDiffState(position));
        //更新源事件列表
        this.filter.refreshOriginList(updateList);
    }
    class BaseRecordFilter extends Filter {
        private final BaseListAdapter m_adapter;
        private List<OneEvent> m_originList;//源数据，所有的事件
        private List<OneEvent> m_filteredList;//过滤后返回的事件列表

        public BaseRecordFilter(BaseListAdapter adapter,List<OneEvent> originList) {
            this.m_adapter=adapter;
            this.m_originList=new ArrayList<>(originList);
            this.m_filteredList=new ArrayList<>();
        }
        public void refreshOriginList(List<OneEvent> originList) {
            this.m_originList=new ArrayList<>(originList);
        }

        @Override
        protected FilterResults performFiltering(CharSequence constraint) {
            // 在程序实际的运行过程中，returnData和originData中的事件列表有可能和数据库中的列表不一致
            // 这是由于adapter中的事件列表和数据库中的事件列表不一致造成的
            // 因此，需要从数据库更新一下originData的数据
            updateDataFromDB();

            String filterStr=constraint.toString().toLowerCase().trim();
            FilterResults results = new FilterResults();
            m_filteredList.clear();
            //如果查询内容为空，就直接返回originData
            if(TextUtils.isEmpty(filterStr)){
                m_filteredList.addAll(m_originList);
            }else {
                for(OneEvent oneEvent:m_originList){
                    if(oneEvent.getCode().contains(filterStr)
                            || oneEvent.getName().contains(filterStr))
                        m_filteredList.add(oneEvent);
                }
            }
            results.values = m_filteredList;
            results.count = m_filteredList.size();
            return results;
        }

        @Override
        protected void publishResults(CharSequence constraint, FilterResults filterResults) {
            m_adapter.mData.clear();
            m_adapter.mData.addAll((ArrayList<OneEvent>)(filterResults.values));
            m_adapter.notifyDataSetChanged();
        }
    }
}