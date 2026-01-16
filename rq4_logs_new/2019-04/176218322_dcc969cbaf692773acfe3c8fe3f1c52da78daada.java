package com.example.lui_project;

import android.database.Cursor;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.util.Log;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseExpandableListAdapter;
import android.widget.ExpandableListView;
import android.widget.ImageView;
import android.widget.TextView;

import java.util.ArrayList;
import java.util.List;

import com.example.lui_project.base.BaseActivity;
import com.example.lui_project.entity.FoodMessage;
import com.example.lui_project.entity.FoodType;
import com.example.lui_project.utils.DBHelper;

public class FoodHotListActivity extends BaseActivity {
    private int sign= - 1 ; //控制列表的展開
    private String[] food_type_array;//食物類型數組
    private List<FoodType> food_list;//數據集合
    private ExpandableListView data_list;//折疊listview
    private Bitmap[] bitmaps;//圖片資源
    private int[] ids;//圖片資源ID數組

    /**
     * set title
     */
    @Override
    protected void setActivityTitle() {
        initTitle();
        setTitle(this.getString(R.string.FoodhotTitle), this);
        setMyBackGround(R.color.watm_background_gray);
        setTitleTextColor(R.color.theme_blue_two);
        setTitleLeftImage(R.mipmap.mrkj_back_blue);
    }

    /**
     *設置界面佈局
     */
    @Override
    protected void getLayoutToView() {
        setContentView(R.layout.activity_food_hot_list);

    }

    /**
     * init
     */
    @Override
    protected void initValues() {
        ids = new int[]{R.mipmap.beantype, R.mipmap.vegetablestype,
        R.mipmap.fruitstype, R.mipmap.meattype, R.mipmap.eggtype,
        R.mipmap.aquatictype, R.mipmap.milktype, R.mipmap.beveragestype,
        R.mipmap.bacterialalgaetype, R.mipmap.fattype};
        bitmaps = new Bitmap[ids.length];
        for (int i = 0;i < ids.length ; i++){
            bitmaps[i] = BitmapFactory.decodeResource(getResources(),ids[i]);
        }
        food_type_array = new String[]{this.getString(R.string.Foodhot_food_type_array1),
                this.getString(R.string.Foodhot_food_type_array2), this.getString(R.string.Foodhot_food_type_array3), this.getString(R.string.Foodhot_food_type_array4),
                this.getString(R.string.Foodhot_food_type_array4), this.getString(R.string.Foodhot_food_type_array6), this.getString(R.string.Foodhot_food_type_array7),
                this.getString(R.string.Foodhot_food_type_array8), this.getString(R.string.Foodhot_food_type_array9), this.getString(R.string.Foodhot_food_type_array10)};
        food_list = new ArrayList<>();
        //struct data scores
        DBHelper dbHelper = new DBHelper();
        Cursor cursor = dbHelper.selectAllDataOfTable("hot");
//        Log.e("among of data", cursor.getCount() + "");
//        int j = 0;
        for (int i = 0; i < 10; i++) {
            FoodType foodType = null;
            List<FoodMessage> foods = null;
            int counts = 1;
            while (cursor.moveToNext()) {
//                Log.e("Count", (++j) + "");
//                Log.e("counts", counts + "");
                String name = cursor.getString(cursor.getColumnIndex("name"));
                String hot = cursor.getString(cursor.getColumnIndex("hot"));
                String type_name = cursor.getString(cursor.getColumnIndex("type_name"));
                if (counts == 1) {
                    foodType = new FoodType();
                    foods = new ArrayList<>();
                    foodType.setFood_type(type_name);
//                    Log.e("type_name", type_name + "");

                }
                FoodMessage foodMessage = new FoodMessage();
                foodMessage.setFood_name(name);
                foodMessage.setHot(hot);
                foods.add(foodMessage);
                foodType.setFood_list(foods);
                if (counts == 20) {
                    food_list.add(foodType);
                    break;
                }
                counts++;
            }
        }
        cursor.close();
        Log.e("data lengths", food_list.size() + "");
//        for (FoodType foodType : food_list) {
//            Log.e("food type", foodType.getFood_type() + "");
//            Log.e("food type",foodType.getFood_list().size() + "");
//        }
    }

    @Override
    protected void initViews() {
        data_list = (ExpandableListView) findViewById(R.id.food_list);
    }
    /**
     * Adapter
     */
    @Override
    protected void setViewsFunction() {
        MyFoodAdapter adapter = new MyFoodAdapter();
        data_list.setAdapter(adapter);
    }
    /**
     * 設置點擊展開一個其餘的都收起
     */
    @Override
    protected void setViewsListener() {
        data_list.setOnGroupClickListener( new  ExpandableListView.OnGroupClickListener() {

            @Override
            public   boolean  onGroupClick(ExpandableListView parent, View v,
                                           int  groupPosition, long  id) {
                // TODO Auto-generated method stub
                if  (sign== - 1 ) {
                    // 展開被選的group
                    data_list.expandGroup(groupPosition);
                    //設置被選中的group置於頂端
                    data_list.setSelectedGroup(groupPosition);
                    sign= groupPosition;
                }  else   if  (sign== groupPosition) {
                    data_list.collapseGroup(sign);
                    sign= - 1 ;
                }  else  {
                    data_list.collapseGroup(sign);
                    // 展開被選的group
                    data_list.expandGroup(groupPosition); // 設置被選中的group放係頂端
                    data_list.setSelectedGroup(groupPosition);
                    sign= groupPosition;
                }
                return   true ;
            }
        });
    }




    /**
     * set Adapter
     */

    class MyFoodAdapter extends BaseExpandableListAdapter {

        //Group的l數量
        @Override
        public int getGroupCount() {
            return food_list.size();
        }
        //每個Group中的Child的數量
        @Override
        public int getChildrenCount(int groupPosition) {
            return food_list.get(groupPosition).getFood_list().size();
        }
        //獲取對應位置的Group
        @Override
        public Object getGroup(int groupPosition) {
            return food_list.get(groupPosition);
        }
        //獲取對應位置中的Child
        @Override
        public Object getChild(int groupPosition, int childPosition) {
            return food_list.get(groupPosition).getFood_list().get(childPosition);
        }
        //獲取對應位置的Group的ID
        @Override
        public long getGroupId(int groupPosition) {
            return groupPosition;
        }
        //獲取對應位置的Child的ID
        @Override
        public long getChildId(int groupPosition, int childPosition) {
            return childPosition;
        }
        //判斷同一個ID是否指向同一個對象
        @Override
        public boolean hasStableIds() {
            return true;
        }
        //獲取Group的view
        @Override
        public View getGroupView(int groupPosition, boolean isExpanded, View convertView, ViewGroup parent) {
            GroupViewHolder holder;
            if (convertView == null){
                holder = new GroupViewHolder();
                convertView = getLayoutInflater().inflate(R.layout.group_item , null);
                holder.image = (ImageView) convertView.findViewById(R.id.group_image);
                holder.title = (TextView) convertView.findViewById(R.id.group_title);
                convertView.setTag(holder);
            }else {
                holder = (GroupViewHolder) convertView.getTag();
            }
            Log.d("image","seting");
            holder.image.setImageBitmap(bitmaps[groupPosition]);
            holder.title.setText(food_type_array[groupPosition]);
            return convertView;
        }
        //獲取child的view
        @Override
        public View getChildView(int groupPosition, int childPosition, boolean isLastChild, View convertView, ViewGroup parent) {
            ChildViewHolder holder;
            if (convertView == null){
                holder = new ChildViewHolder();
                convertView = getLayoutInflater().inflate(R.layout.child_item,null);
                holder.name = (TextView) convertView.findViewById(R.id.food_name);
                holder.hot = (TextView) convertView.findViewById(R.id.food_hot);
                convertView.setTag(holder);
            }else {
                holder = (ChildViewHolder) convertView.getTag();
            }
            FoodMessage food = food_list.get(groupPosition).getFood_list().get(childPosition);
            holder.name.setText(food.getFood_name());
            holder.hot.setText(food.getHot()+FoodHotListActivity.this.getString(R.string.Foodhot_hot));
            return convertView;
        }

        //判斷child是唔可以被選擇
        @Override
        public boolean isChildSelectable(int groupPosition, int childPosition) {
            return true;
        }
    }

    class GroupViewHolder{
        ImageView image;
        TextView title;
    }
    class ChildViewHolder{
        TextView name,hot;
    }
}