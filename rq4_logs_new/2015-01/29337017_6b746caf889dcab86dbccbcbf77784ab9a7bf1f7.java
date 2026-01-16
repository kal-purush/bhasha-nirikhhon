package com.icode.library.widgets.adapter;

import java.util.ArrayList;

import android.content.Context;
import android.graphics.drawable.Drawable;
import android.view.LayoutInflater;
import android.view.View;
import android.view.View.OnClickListener;
import android.view.ViewGroup;
import android.widget.BaseExpandableListAdapter;
import android.widget.ImageView;
import android.widget.TextView;

import com.code.icodelibrary.R;
import com.icode.library.tools.utils.IStateDrawableUtils;

/**
 * 对分组展示adapter的简单实现
 */
public class ISimpleExpandableAdapter extends BaseExpandableListAdapter {

  private ArrayList<IExpandableItem> items = new ArrayList<IExpandableItem>();
  private static final int DISABLE_CODE = -1 ;
  private Context mContext;
  private IExpandableInfo expandableInfo;

  public ISimpleExpandableAdapter(Context context,IExpandableInfo expandableInfo, ArrayList<IExpandableItem> items) {
    this.mContext = context;
    this.items = items;
    this.expandableInfo = expandableInfo;
  }

  public static interface IExpandableItemClickedListener {
    
    void expandableItemClicked(boolean isGroup,int groupPosition,int childPosition,IExpandableItem expandableItem);
    
  }
  
  public static class IExpandableInfo{
    
    public int groupIndicatorUp = DISABLE_CODE ;
    public int groupIndicatorDown = DISABLE_CODE;
    public int  groupBgnormal = DISABLE_CODE ;
    public int groupBgPressed = DISABLE_CODE;
    public int childBgNormal = DISABLE_CODE ;
    public int childBgPressed = DISABLE_CODE;
    public int childIndicator = DISABLE_CODE;
    public IExpandableItemClickedListener clickedListener ;
    
    
    public static IExpandableInfo getExpandableInfo(Context context){
      IExpandableInfo info = new IExpandableInfo();
      
      info.childBgNormal = R.drawable.common_setting_item_singleline_pressed;
      info.childBgPressed = R.drawable.common_setting_item_singleline_pressed_darker;
      info.groupIndicatorUp = R.drawable.common_expandable_group_indicator_up;
      info.groupIndicatorDown = R.drawable.common_expandable_group_indicator_down;
      info.groupBgnormal =  R.drawable.common_setting_item_singleline_normal;
      info.groupBgPressed =  R.drawable.common_setting_item_singleline_pressed;
      info.clickedListener = null;
      
      return info;
      
    }
  }
  
  
  public static class IExpandableItem {

    private String itemId;
    private String itemName;
    private String url = "";
    private Object extraInfo;

    private ArrayList<IExpandableItem> expandableItems = new ArrayList<IExpandableItem>();

    public static IExpandableItem getInstance(String itemId, String itemName, String url,
        String extraInfo) {
      IExpandableItem item = new IExpandableItem();
      item.itemId = itemId;
      item.extraInfo = extraInfo;
      item.itemName = itemName;
      item.url = url;
      return item;

    }

    public static IExpandableItem getInstance(String itemName) {

      IExpandableItem item = new IExpandableItem();
      item.itemName = itemName;
      return item;

    }

    public static IExpandableItem getInstance(String itemName,
        ArrayList<IExpandableItem> expandableItems) {

      IExpandableItem item = new IExpandableItem();
      item.itemName = itemName;
      item.expandableItems = expandableItems;
      return item;

    }

    public static IExpandableItem getInstance(String itemName, String extraInfo) {

      IExpandableItem item = new IExpandableItem();
      item.extraInfo = extraInfo;
      item.itemName = itemName;
      return item;

    }

    public String getItemId() {
      return itemId;
    }

    public void setItemId(String itemId) {
      this.itemId = itemId;
    }

    public String getItemName() {
      return itemName;
    }

    public void setItemName(String itemName) {
      this.itemName = itemName;
    }

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public Object getExtraInfo() {
      return extraInfo;
    }

    public void setExtraInfo(Object extraInfo) {
      this.extraInfo = extraInfo;
    }

    public ArrayList<IExpandableItem> getExpandableItems() {
      return expandableItems;
    }

    public void setExpandableItems(ArrayList<IExpandableItem> expandableItems) {
      this.expandableItems = expandableItems;
    }

  }

  @Override
  public int getGroupCount() {

    return items.size();
  }

  @Override
  public int getChildrenCount(int groupPosition) {

    return items.get(groupPosition).getExpandableItems().size();
  }

  @Override
  public Object getGroup(int groupPosition) {

    return items.get(groupPosition);
  }

  @Override
  public Object getChild(int groupPosition, int childPosition) {

    return items.get(groupPosition).getExpandableItems().get(childPosition);
  }

  @Override
  public long getGroupId(int groupPosition) {

    return groupPosition;
  }

  @Override
  public long getChildId(int groupPosition, int childPosition) {

    return groupPosition * childPosition;
  }

  @Override
  public boolean hasStableIds() {

    return false;
  }

  @Override
  public View getGroupView(final int groupPosition, boolean isExpanded, View convertView, ViewGroup parent) {

    final IExpandableItem item = items.get(groupPosition);
    GroupHolder groupHolder;
    if (convertView == null) {
      convertView = LayoutInflater.from(mContext).inflate(
          R.layout.common_expandable_listview_group, null);
      groupHolder = new GroupHolder();
      groupHolder.textView_name = (TextView) convertView
          .findViewById(R.id.expandable_listview_group_name);
      groupHolder.imageView_arrow = (ImageView)convertView.findViewById(R.id.expandable_listview_group_indicator);
      convertView.setTag(groupHolder);
    } else {
      groupHolder = (GroupHolder) convertView.getTag();
    }
    groupHolder.textView_name.setText(item.getItemName());
    if(!isExpanded){
      groupHolder.imageView_arrow.setImageResource(expandableInfo.groupIndicatorDown);
    }else {
      groupHolder.imageView_arrow.setImageResource(expandableInfo.groupIndicatorUp);  
    }

    convertView.setBackgroundDrawable(
        IStateDrawableUtils.newSelector(mContext, expandableInfo.groupBgnormal,
            expandableInfo.groupBgPressed));
    
    return convertView;
  }

  @Override
  public View getChildView(final int groupPosition, final int childPosition, boolean isLastChild,
      View convertView, ViewGroup parent) {
   final IExpandableItem item = items.get(groupPosition).getExpandableItems().get(childPosition);
    ChildHolder childHolder;
    if (convertView == null) {
      convertView = LayoutInflater.from(mContext).inflate(
          R.layout.common_expandable_listview_child, null);
      childHolder = new ChildHolder();
      childHolder.imageView = (ImageView) convertView
          .findViewById(R.id.expandable_listview_child_img);
      childHolder.textView_name = (TextView) convertView
          .findViewById(R.id.expandable_listview_child_name);
      convertView.setTag(childHolder);
    } else {
      childHolder = (ChildHolder) convertView.getTag();
    }
    
    convertView.setBackgroundDrawable(IStateDrawableUtils.newSelector(mContext,
        expandableInfo.childBgNormal, expandableInfo.childBgPressed));
    
    childHolder.textView_name.setText(item.getItemName() );
    if(expandableInfo.childIndicator!=DISABLE_CODE){
      childHolder.imageView.setImageResource(expandableInfo.childIndicator);
    }
    
    if(expandableInfo.clickedListener!=null){
      
      convertView.setOnClickListener(new OnClickListener() {
        
        @Override
        public void onClick(View v) {
          expandableInfo.clickedListener.expandableItemClicked(false,groupPosition,
              childPosition, item);
        }
      });
    }

    return convertView;
  }

  @Override
  public boolean isChildSelectable(int groupPosition, int childPosition) {

    return false;
  }

  private class GroupHolder {
    private ImageView imageView_arrow ;
    private TextView textView_name;

  }

  private class ChildHolder {

    private ImageView imageView;
    private TextView textView_name;

  }

}