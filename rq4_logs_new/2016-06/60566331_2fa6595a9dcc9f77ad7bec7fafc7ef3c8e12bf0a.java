package org.ragecastle.cli4p.Contacts_View;

import android.app.Activity;
import android.content.Context;
import android.database.Cursor;
import android.provider.ContactsContract;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.CursorAdapter;
import android.widget.TextView;

import org.ragecastle.cli4p.R;

/**
 * Created by jahall on 6/18/16.
 *
 * Array Adapter to display the contact name in a list view
 *
 */
public class ContactsAdapter extends CursorAdapter {

    private static final String LOG_TAG = ContactsAdapter.class.getSimpleName();
    private int lastPosition;
    private TextView textView;

    public ContactsAdapter(Activity context, Cursor cursor, int flags) {
        super(context, cursor, flags);
    }

    private String getContact(Cursor cursor){

        return cursor.getString(
                cursor.getColumnIndex(
                        ContactsContract.Contacts.DISPLAY_NAME_PRIMARY));
    }
//
//    @Override
//    public View getView(int position, View convertView, ViewGroup parent) {
//        lastPosition = position;
//        String contact = getItem(position);
//
//        if(convertView == null){
//            convertView = LayoutInflater.from(getContext())
//                    .inflate(R.layout.list_item_contact, parent, false);
//        }
//
//        textView = (TextView) convertView.findViewById(R.id.contact_name);
//        textView.setText(contact);
//
//        return convertView;
//    }

    @Override
    public View newView(Context context, Cursor cursor, ViewGroup parent) {
        return LayoutInflater.from(context)
                .inflate(R.layout.list_item_contact, parent, false);
    }

    @Override
    public void bindView(View view, Context context, Cursor cursor) {
        textView = (TextView) view.findViewById(R.id.contact_name);
        textView.setText(getContact(cursor));
    }

//    @Override
//    public void add(String contact) {
//        textView.setText(contact);
//    }

    @Override
    public Cursor swapCursor(Cursor newCursor) {
        return super.swapCursor(newCursor);
    }
}