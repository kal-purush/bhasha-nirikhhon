package com.itau.mygod.ui;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.widget.LinearLayout;
import android.widget.ListView;
import android.widget.RelativeLayout;
import android.widget.TextView;
import android.widget.AdapterView.OnItemLongClickListener;

import cn.bmob.v3.BmobQuery;
import cn.bmob.v3.datatype.BmobPointer;
import cn.bmob.v3.exception.BmobException;
import cn.bmob.v3.listener.FindListener;

import android.widget.SimpleAdapter;
import com.itau.jingdong.R;
import com.itau.mygod.user.User;
import com.itau.mygod.user.Product;
import com.itau.mygod.user.Order;

public class OrderActivity extends Activity{
	
	private static final String Tag="OrderActivity";
	//private OrderActivity orderActivity=null;
	private SimpleAdapter adapter;
 	private List<Map<String,Object>> datalist;
	ListView listview;
	
	
	protected void onCreate(Bundle savedInstanceState)
	{
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_order);
		findViewById();
		
		datalist = new ArrayList<Map<String,Object>>();
		
 		int [] to = {R.id.tv_title,R.id.tv_describe,R.id.tv_price,R.id.tv_area,R.id.tv_phone};
 		String from[] = {"title","description","price","area","phone"};	
 		
 		adapter = new SimpleAdapter(this, getProIn(),R.layout.order_list, from, to);		
 		listview.setAdapter(adapter);
		
		//orderActivity=OrderActivity.this;

	}
	

   protected void findViewById(){
	   
		listview = (ListView) findViewById(R.id.list_buyer);
	   
   }
	

	public List<Map<String,Object>> getProIn(){
	 		BmobQuery<Order> query = new BmobQuery<Order>();
	 		query.addWhereEqualTo("status", 1);
	 		//query.setLimit(50);
	 		query.findObjects(new FindListener<Order>() 
	 		{
	 			
	 			@Override
	 			public void done(List<Order> object, BmobException e)
	 			{
	 				for (Order order : object)
	 				{
	 					Map<String,Object> map = new HashMap<String, Object>();
	 					map.put("description", order.getDescription());
	 					map.put("title", order.getTitle());
	 					map.put("price", order.getPrice());
	 					map.put("area", order.getArea());
	 					map.put("phone",order.getPhone());
	 					datalist.add(map);		               
	 		         }
	 				adapter.notifyDataSetChanged();				
	 			}
	 	
	 		});		
	 		
	 		return datalist;
	 	}


}