package com.itau.mygod.ui;

import android.app.Activity;
import android.app.AlertDialog;
import android.os.Bundle;
import cn.bmob.v3.BmobUser;
import cn.bmob.v3.exception.BmobException;
import cn.bmob.v3.listener.SaveListener;

import com.itau.jingdong.R;

import android.app.Activity;
import android.content.DialogInterface;
import android.content.Intent;
import android.net.ConnectivityManager;
import android.os.AsyncTask;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.ImageView;
import com.itau.mygod.user.Product;
import com.itau.mygod.user.Order;
import com.itau.mygod.user.User;


public class ProductDetailActivity extends Activity implements OnClickListener{
	
	private static final String Tag="ProductDeatilActivity";
	private ProductDetailActivity productBuyActivity=null;
	private Button productBuy;
	
	private String title;
	private String price;
	private String area;
	private String description;
	private String pId;
	
	
	@Override
	protected void onCreate(Bundle savedInstanceState)
	{
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_product_detail);
		
		findViewById();
		initView();
	}
	
	protected void findViewById(){
		productBuy=(Button)this.findViewById(R.id.btnbuy);
		
	}
	
	protected void initView(){
		productBuy.setOnClickListener(this);
	}

	@Override
	public void onClick(View v) {
		// TODO Auto-generated method stub
		productBuy(v);
	}
	
	public void productBuy(View view){
		
		new AlertDialog.Builder(ProductDetailActivity.this).setTitle("系统提示")//设置对话框标题  
		  
	     .setMessage("确定提交订单吗")//设置显示的内容  
	  
	     .setPositiveButton("确定",new DialogInterface.OnClickListener() {//添加确定按钮  
	   
	         @Override  
	  
	         public void onClick(DialogInterface dialog, int which) {//确定按钮的响应事件  
	  
		        //获得商品和买家信息
	        	 Order order=new Order();
	        	 Product product=new Product();
	        	 User user=BmobUser.getCurrentUser(User.class);
	        	 
	        	 title=product.getTitle();
	        	 price=product.getPrice();
	        	 description=product.getDescription();
	        	 area=product.getArea();
	        	 pId=product.getObjectId();
	        	 
	        	 order.setBuyerId(user);
	        	 order.setTitle(title);
	        	 order.setDescription(description);
	        	 order.setPrice(price);
	        	 order.setArea(area);
	        	 
	        	 order.save(new SaveListener<String>(){

					@Override
					public void done(String objectId, BmobException e) {
						// TODO Auto-generated method stub
						if(e==null){
							Log.i("Bmob","保存成功");
						}
						else{
							Log.i("Bmob","保存失败"+e.getMessage());
						}
						
					}
	        		 
	        	 });
	        	 
	        	//跳转到订单页
		        startActivity(new Intent(ProductDetailActivity.this, OrderActivity.class));
		        //ProductDetailActivity.this.finish();
	  
	         }  
	  
	     }).setNegativeButton("返回",new DialogInterface.OnClickListener() {

			@Override
			public void onClick(DialogInterface dialog, int which) {
				// TODO Auto-generated method stub
				 finish();
				
			}//返回按钮       
	  
	     });
	  
	 }  	
		
	}
