package com.sasincomm.quickcontacts;

import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.os.Bundle;
import android.provider.ContactsContract;
import android.text.TextUtils;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import com.sasincomm.quickcontacts.base.BaseActivity;
import com.sasincomm.quickcontacts.model.Contact;
import com.sasincomm.quickcontacts.utils.Logger;

import org.greenrobot.eventbus.EventBus;

import java.io.InputStream;

import de.hdodenhof.circleimageview.CircleImageView;

/**
 * 作者：wk on 2018/12/25.
 */
public class ContactInfoActivity extends BaseActivity {
    public static final String TAG="ContactInfoActivity";

    private Button backBtn,updateBtn,deleteBtn,editBtn;
    private CircleImageView photoIV;
    private TextView titleTV;
    private EditText nameET, numberET, emailET;

    private Contact mContact;

    @Override
    protected int getContentLayoutId() {
        return R.layout.activity_contact_info;
    }

    @Override
    protected void initWidget() {
        backBtn=findViewById(R.id.btn_back_left);
        titleTV=findViewById(R.id.tv_back_center);
        updateBtn=findViewById(R.id.btn_back_update);
        deleteBtn=findViewById(R.id.btn_back_delete);
        editBtn=findViewById(R.id.btn_back_right);
        photoIV=findViewById(R.id.iv_contact_info_head);
        nameET =findViewById(R.id.et_contact_info_name);
        numberET =findViewById(R.id.et_contact_info_number);
        emailET =findViewById(R.id.et_contact_info_email);
    }

    @Override
    protected void initEvent() {
        //返回
        backBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                finish();
            }
        });
        //更新按钮点击响应事件
        updateBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String name= nameET.getText().toString().trim();
                String number= numberET.getText().toString().trim();
                String email= emailET.getText().toString().trim();
                Uri uri = Uri.parse("content://com.android.contacts/data");//对data表的所有数据操作
                ContentResolver resolver = getContentResolver();
                ContentValues values = new ContentValues();
                boolean isUpdateSuccess=true;
                //更新email
                if(TextUtils.isEmpty(mContact.getEmail()))
                {
                    //插入email
                    values.clear();
                    values.put("data1", email);
                    values.put("raw_contact_id", mContact.getRawId());
                    values.put(ContactsContract.Data.MIMETYPE,"vnd.android.cursor.item/email_v2");
                    resolver.insert(uri, values);
                }else
                {
                    //更新email
                    if(!mContact.getEmail().equals(email))
                    {
                        values.clear();
                        values.put("data1", email);
                        int emailRow=resolver.update(uri, values, "mimetype=? and raw_contact_id=?",
                                new String[]{"vnd.android.cursor.item/email_v2",String.valueOf(mContact.getRawId())});
                        Logger.d(TAG,"update email emailRow:"+emailRow);
                        if(emailRow==0)isUpdateSuccess=false;
                    }
                }
                //更新name
                if(TextUtils.isEmpty(mContact.getName()))
                {
                    //插入name
                    values.clear();
                    values.put("data1", name);
                    values.put("raw_contact_id", mContact.getRawId());
                    values.put(ContactsContract.Data.MIMETYPE,"vnd.android.cursor.item/name");
                    resolver.insert(uri, values);
                }else
                {
                    //更新name
                    if(!mContact.getName().equals(name))
                    {
                        values.clear();
                        values.put("data1", name);
                        int nameRow=resolver.update(uri, values, "mimetype=? and raw_contact_id=?",
                                new String[]{"vnd.android.cursor.item/name",String.valueOf(mContact.getRawId())});
                        Logger.d(TAG,"update name nameRow:"+nameRow);
                        if(nameRow==0)isUpdateSuccess=false;
                    }
                }
                //更新number
                if(TextUtils.isEmpty(mContact.getPhoneNum()))
                {
                    //插入number
                    values.clear();
                    values.put("data1", number);
                    values.put("raw_contact_id", mContact.getRawId());
                    values.put(ContactsContract.Data.MIMETYPE,"vnd.android.cursor.item/phone_v2");
                    resolver.insert(uri, values);
                }else
                {
                    //更新number
                    if(!number.equals(mContact.getPhoneNum()))
                    {
                        values.clear();
                        values.put("data1", number);
                        int numberRow=resolver.update(uri, values, "mimetype=? and raw_contact_id=?",
                                new String[]{"vnd.android.cursor.item/phone_v2",String.valueOf(mContact.getRawId())});
                        Logger.d(TAG,"update number numberRow:"+numberRow);
                        if(numberRow==0)isUpdateSuccess=false;
                    }
                }
                if(isUpdateSuccess)
                {
                    //发送事件更新
                    EventBus.getDefault().post("update");
                    finish();
                    Toast.makeText(ContactInfoActivity.this,"更新成功",Toast.LENGTH_SHORT).show();
                }else
                {
                    Toast.makeText(ContactInfoActivity.this,"更新失败",Toast.LENGTH_SHORT).show();
                }
            }
        });
        //删除按钮点击响应事件
        deleteBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                ContentResolver contentResolver = getContentResolver();
                Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
                int rows=contentResolver.delete(uri,"_id=?", new String[]{String.valueOf(mContact.getRawId())});
                if(rows>0)
                {
                    //发送事件更新
                    EventBus.getDefault().post("update");
                    finish();
                    Toast.makeText(ContactInfoActivity.this,"删除成功",Toast.LENGTH_SHORT).show();
                }else
                {
                    Toast.makeText(ContactInfoActivity.this,"删除失败",Toast.LENGTH_SHORT).show();
                }
            }
        });
        //编辑按钮点击响应事件
        editBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                switch (editBtn.getText().toString().trim())
                {
                    case "编辑":
                        editBtn.setText("保存");
                        updateBtn.setVisibility(View.VISIBLE);
                        deleteBtn.setVisibility(View.VISIBLE);
                        break;
                    case "保存":
                        editBtn.setText("编辑");
                        updateBtn.setVisibility(View.GONE);
                        deleteBtn.setVisibility(View.GONE);
                        break;
                }
            }
        });
    }

    @Override
    protected void initData() {
        titleTV.setText("联系人详情");
        //获取传进来的联系人数据
        mContact = (Contact) getIntent().getBundleExtra("contact_bundle").get("contact");
        if (mContact != null) {
            //显示图像
            if(mContact.getPhotoId()>0)
            {
                Uri uri = ContentUris.withAppendedId(
                        ContactsContract.Contacts.CONTENT_URI,
                        mContact.getId());
                InputStream input = ContactsContract.Contacts
                        .openContactPhotoInputStream(getContentResolver(), uri);
                Bitmap contactPhoto = BitmapFactory.decodeStream(input);
                photoIV.setImageBitmap(contactPhoto);
            }else
            {
                photoIV.setImageResource(R.mipmap.ic_launcher);
            }
            //显示姓名
            if(TextUtils.isEmpty(mContact.getName()))
            {
                nameET.setText("未知联系人");
            }else
            {
                nameET.setText(mContact.getName());
            }
            //显示电话号码
            numberET.setText(mContact.getPhoneNum());
            //显示邮箱
            ContentResolver contentResolver = this.getContentResolver();
            Cursor emailCursor = contentResolver.query(ContactsContract.CommonDataKinds.Email.CONTENT_URI,
                    new String[]{ContactsContract.CommonDataKinds.Email.DATA},
                    ContactsContract.CommonDataKinds.Email.RAW_CONTACT_ID + "=?", new String[]{String.valueOf(mContact.getRawId())}, null);
            while(emailCursor.moveToNext()) {
                String email=emailCursor.getString(0);
                mContact.setEmail(email);
                emailET.setText(email);
                Logger.d(TAG,"接收邮箱:"+email);
            }
            emailCursor.close();
        }
    }

    /**
     * 跳转到通话详情界面
     * @param contact
     * @param context
     */
    public static void jumpToContactInfoView(Contact contact,Context context)
    {
        Intent intent=new Intent(context,ContactInfoActivity.class);
        Bundle bundle=new Bundle();
        bundle.putSerializable("contact",contact);
        intent.putExtra("contact_bundle",bundle);
        context.startActivity(intent);
    }

}