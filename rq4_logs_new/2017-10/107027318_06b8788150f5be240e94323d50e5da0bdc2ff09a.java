package com.itau.mygod.product;

import java.util.List;

import com.google.zxing.client.result.ProductParsedResult;
import com.itau.jingdong.R;
import com.itau.mygod.ui.IndexActivity;
import com.itau.mygod.ui.IndexShakeActivity;
import com.itau.mygod.ui.LoginActivity;
import com.itau.mygod.ui.RegisterNormalActivity;
import com.itau.mygod.user.User;

import android.R.string;
import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.AdapterView;
import android.widget.AdapterView.OnItemSelectedListener;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.Spinner;
import android.widget.Toast;
import cn.bmob.v3.BmobQuery;
import cn.bmob.v3.exception.BmobException;
import cn.bmob.v3.listener.FindListener;
import cn.bmob.v3.listener.SaveListener;

public class HistoryActivity extends Activity{
	
	private EditText product_name, product_price,product_description;
	private Spinner product_class,product_area;
	private ImageButton product_image;
	private Button product_submit;
	private Activity historyActivity;
	private Intent intent;
	private String productName;
	private String productPrice;
	private String productClass;
	private String productDescription;
	private String productArea;
	

	

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_history);
		product_name=(EditText) findViewById(R.id.product_name_text);
		product_class=(Spinner) findViewById(R.id.product_class_spinner);
		product_price=(EditText) findViewById(R.id.product_price_text);
		product_image=(ImageButton) findViewById(R.id.product_camer_button);
		product_submit=(Button) findViewById(R.id.product_submit_button);
		product_description=(EditText) findViewById(R.id.product_description_text);
		product_area=(Spinner) findViewById(R.id.product_area_spinner);
		//下拉框
		productArea = (String) product_area.getSelectedItem();
		productClass = (String) product_class.getSelectedItem();
		
		//product_submit.setOnClickListener(this);
		historyActivity=HistoryActivity.this;
		//监听获得商品种类
		product_class.setOnItemSelectedListener(new OnItemSelectedListener() {
			@Override
			public void onItemSelected(AdapterView<?> parent, View view, int position ,long id){
				productClass = (String) product_class.getSelectedItem();
				
			}
			@Override
			public void onNothingSelected(AdapterView<?> parent){
				
			}
		});
		//监听获得地点信息
		product_area.setOnItemSelectedListener(new OnItemSelectedListener() {
			@Override
			public void onItemSelected(AdapterView<?> parent, View view, int position ,long id){
				productArea = (String) product_area.getSelectedItem();
				
			}
			@Override
			public void onNothingSelected(AdapterView<?> parent){
				
			}
		});
		//监听提交按钮
		product_submit.setOnClickListener(new OnClickListener() {
			
			@Override
			public void onClick(View arg0) {
				// TODO Auto-generated method stub
				productName=product_name.getText().toString();
				productPrice=product_price.getText().toString();
				productDescription=product_description.getText().toString();
				addProduct();
				intent=new Intent(HistoryActivity.this,IndexShakeActivity.class);
				startActivity(intent);
			}
		});
	}
	
	public void addProduct() {
        if(productName.length() == 0)
		{
			Toast.makeText(getBaseContext(), "请输入商品名称", Toast.LENGTH_LONG).show();
		}
        else if (productPrice.length() == 0) {
        	Toast.makeText(getBaseContext(), "请输入价格", Toast.LENGTH_LONG).show();
		}
		else
		{
			final Product product = new Product();
			product.setTitle(product_name.getText().toString());
			product.setType(productClass);
			product.setPrice(productPrice);
			product.setDescription(productDescription);
			product.setArea(productArea);
			BmobQuery<Product> query = new BmobQuery<Product>();
			query.addWhereEqualTo("title", product_name.getText().toString());
			query.addWhereEqualTo("type", productClass);
			query.addWhereEqualTo("price", productPrice);
			query.addWhereEqualTo("description", productDescription);
			query.addWhereEqualTo("area", productArea);
			query.findObjects(new FindListener<Product>() {
				
				@Override
				public void done(List<Product> object, BmobException e) {
					
						product.save(new SaveListener<String>() {
							
							@Override
							public void done(String objectId, BmobException e) {
								if(e == null)
								{
									Toast.makeText(getBaseContext(), "发布成功!", Toast.LENGTH_SHORT).show();
								}
								else
								{
									Toast.makeText(getBaseContext(), "发布失败!", Toast.LENGTH_LONG).show();
								}
							}
						});
					
					
					
				}
			});
		}

		
	}
	public boolean isNum(String str)
	{
		if (str.charAt(0)-'0'==0) {
			return false;
			
		}
		for (int i = 0; i < str.length(); i++) {
			
			int chr=str.charAt(i);
			if (chr<48||chr>57) {
				return false;
			}
		}
		return true;
	}
	
}