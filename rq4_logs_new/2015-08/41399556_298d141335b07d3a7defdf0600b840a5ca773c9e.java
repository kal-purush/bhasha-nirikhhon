
package com.softtanck.framework.activity;

import android.widget.ListAdapter;

import com.softtanck.framework.R;
import com.softtanck.framework.adapter.SchoolBagAndSignUpExpandAdapter;
import com.softtanck.framework.bean.ChildCourseAndSignUpInfo;
import com.softtanck.framework.bean.CourseAndSignUpInfo;
import com.softtanck.framework.pulltorefresh.PullToRefreshExpandableListView;

import java.util.List;


/**
 * 我的书包和闪电报名的页面
 */
public class MySchoolBagAndSignUpActivity extends BaseActivity  {

    private PullToRefreshExpandableListView pullRefreshExpandableList;
    private SchoolBagAndSignUpExpandAdapter adapter;
    private List<CourseAndSignUpInfo>list;


    @Override
    protected int getViewId() {
        return R.layout.activity_my_school_bag_and_sign_up;
    }

    @Override
    protected void onActivityCreate() {
        initExpandListView();

    }

    private void initExpandListView() {
        pullRefreshExpandableList=(PullToRefreshExpandableListView)findViewById(R.id.pull_refresh_expandable_list);
        CourseAndSignUpInfo info=new CourseAndSignUpInfo();
        info.setParentType("其他");
        info.setParentTime("3天后过期");
        info.setParentContent("MOTO x拆装机教程");
        ChildCourseAndSignUpInfo child0=new ChildCourseAndSignUpInfo();
        child0.setChildType(0);
        child0.setChildContent("MOTO x拆装机教程");
        ChildCourseAndSignUpInfo child1=new ChildCourseAndSignUpInfo();
        child1.setChildType(2);
        child1.setChildContent("wifi下观看视频moto拆装");
        info.getList().add(child0);
        info.getList().add(child1);


        CourseAndSignUpInfo info1=new CourseAndSignUpInfo();
        info1.setParentType("其他");
        info1.setParentTime("2015-10-31过期");
        info1.setParentContent("必修课：A330产品功能介绍");
        ChildCourseAndSignUpInfo c0=new ChildCourseAndSignUpInfo();
        c0.setChildType(0);
        c0.setChildContent("A330产品功能介绍");
        ChildCourseAndSignUpInfo c1=new ChildCourseAndSignUpInfo();
        c1.setChildType(1);
        c1.setChildContent("1、A330产品功能介绍(一)");
        ChildCourseAndSignUpInfo c2=new ChildCourseAndSignUpInfo();
        c1.setChildType(1);
        c1.setChildContent("2、A330产品功能介绍(二)");
        ChildCourseAndSignUpInfo c3=new ChildCourseAndSignUpInfo();
        c1.setChildType(1);
        c1.setChildContent("3、A330产品功能介绍(三)");
        info1.getList().add(c0);
        info1.getList().add(c1);
        info1.getList().add(c2);
        info1.getList().add(c3);

        list.add(info);
        list.add(info1);
        adapter=new SchoolBagAndSignUpExpandAdapter(context,list);
        pullRefreshExpandableList.setAdapter((ListAdapter)adapter);

    }
}