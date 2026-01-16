package com.ufo.ufomobile.reportesmoviles;

import android.app.Activity;
import android.app.Dialog;
import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.os.Bundle;
import android.provider.MediaStore;
import android.support.v4.app.DialogFragment;
import android.support.v4.app.FragmentTransaction;
import android.util.Base64;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.view.Window;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.ImageView;
import android.widget.Toast;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by Tiago on 15/08/16.
 */
public class EditProfileDialogFragment extends DialogFragment {
    private int PICK_IMAGE_REQUEST = 1;

    private String actName,actIdNumber,actPhone,actPlace,actPicture,newPicture;
    private EditText name,idNumber,phone,place;
    private ImageView picture;
    private Button done;
    OnaAddSelected mListener;
    private Bitmap bitmap;
    private Uri filePath;

    public interface OnaAddSelected {
        public void onArticleSelectedListener(String name,String idNum,String phone,String place,String picture);
    }

    public EditProfileDialogFragment(){

    }

    public static EditProfileDialogFragment newInstance(String name,String idNum,String phone,String place, String picture){
        EditProfileDialogFragment f = new EditProfileDialogFragment();

        // Supply num input as an argument.
        Bundle args = new Bundle();
        args.putString("name",name);
        args.putString("idNum",idNum);
        args.putString("phone",phone);
        args.putString("place",place);
        args.putString("picture",picture);
        f.setArguments(args);
        return f;
    }

    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        // The only reason you might override this method when using onCreateView() is
        // to modify any dialog characteristics. For example, the dialog includes a
        // title by default, but your custom layout might not need it. So here you can
        // remove the dialog title, but you must call the superclass to get the Dialog.
        Dialog dialog = super.onCreateDialog(savedInstanceState);
        dialog.requestWindowFeature(Window.FEATURE_NO_TITLE);
        return dialog;
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout to use as dialog or embedded fragment
        View view=inflater.inflate(R.layout.edit_profile_dialog, container, false);
        //------------------------------------------------------------------------------------------
        actName=getArguments().getString("name");
        actIdNumber=getArguments().getString("idNum");
        actPhone=getArguments().getString("phone");
        actPlace=getArguments().getString("place");
        actPicture=getArguments().getString("picture");
        newPicture=actPicture;
        //------------------------------------------------------------------------------------------
        name=(EditText)view.findViewById(R.id.name);
        idNumber=(EditText)view.findViewById(R.id.id_number);
        phone=(EditText)view.findViewById(R.id.phone);
        place=(EditText)view.findViewById(R.id.place);
        picture=(ImageView)view.findViewById(R.id.pic);

        name.setText(actName);
        idNumber.setText(actIdNumber);
        phone.setText(actPhone);
        place.setText(actPlace);

        done=(Button)view.findViewById(R.id.done);
        done.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String nam=name.getText().toString();
                String idNum= idNumber.getText().toString();
                String pho=phone.getText().toString();
                String plc=place.getText().toString();
                if(!nam.equals("") && !idNum.equals("") && !pho.equals("") && !plc.equals("")) {
                    mListener.onArticleSelectedListener(nam, idNum, pho, plc,newPicture);
                    dismiss();
                }else{
                    Toast.makeText(getActivity(),
                            getResources().getString(R.string.incomplete_info_error),Toast.LENGTH_SHORT).show();
                }
            }
        });

        if(!actPicture.equals("") && actPicture!=null){
            Bitmap imag = null;
            byte[] decodedString = Base64.decode(actPicture, Base64.DEFAULT);
            imag = BitmapFactory.decodeByteArray(decodedString, 0, decodedString.length);
            picture.setImageBitmap(imag);
        }

        picture.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                showFileChooser();
            }
        });
        return view;
    }

    @Override
    public void onAttach(Activity activity) {
        super.onAttach(activity);
        try {
            mListener = (OnaAddSelected) activity;
        } catch (ClassCastException e) {
            throw new ClassCastException(activity.toString() + " must implement OnArticleSelectedListener");
        }
    }

    private void showFileChooser() {
        Intent intent = new Intent();
        intent.setType("image/*");
        intent.setAction(Intent.ACTION_GET_CONTENT);
        startActivityForResult(Intent.createChooser(intent, "Select Picture"), PICK_IMAGE_REQUEST);
    }

    @Override
    public void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);

        if (requestCode == PICK_IMAGE_REQUEST && resultCode == getActivity().RESULT_OK && data != null && data.getData() != null) {
            filePath = data.getData();
            try {
                bitmap = MediaStore.Images.Media.getBitmap(getActivity().getContentResolver(), filePath);
                Bitmap bitmap2 = Bitmap.createScaledBitmap(bitmap,240,240,false);
                picture.setImageBitmap(bitmap2);
                newPicture=getStringImage(bitmap2);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String getStringImage(Bitmap bmp){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        bmp.compress(Bitmap.CompressFormat.JPEG, 100, baos);
        byte[] imageBytes = baos.toByteArray();
        String encodedImage = Base64.encodeToString(imageBytes, Base64.DEFAULT);
        return encodedImage;
    }
}