package easyrun.activity;

import android.app.Fragment;
import android.app.FragmentTransaction;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import com.special.ResideMenu.ResideMenu;

import easyrun.bean.UserBean;
import easyrun.util.R;


/**
 * Created by J_Crocodile on 2016/6/20.
 */
public class UserUploadSuccessFragment extends Fragment{
    private Button UploadPicAgain;
    private Button Call;
    private View parentView;
    private ResideMenu resideMenu;
    private String account;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        getActivity().getActionBar().setTitle("上传成功");
        getActivity().getActionBar().show();
        parentView = inflater.inflate(R.layout.user_uploadpic_success, container, false);
        MainActivity parentActivity = (MainActivity) getActivity();
        resideMenu = parentActivity.getResideMenu();
        Bundle bundle = getArguments();
        account=bundle.getString("account");
        UploadPicAgain=(Button)parentView.findViewById(R.id.uploadAgain);
        Call=(Button)parentView.findViewById(R.id.Call);
        // add gesture operation's ignored views
        Call.setOnClickListener(mylistener);
        UploadPicAgain.setOnClickListener(mylistener);
        return parentView;
    }

    Button.OnClickListener mylistener = new Button.OnClickListener(){
        @Override
        public void onClick(View v) {
            //鼠标点击菜单项进入相应的子商城
            switch (v.getId()){
                case R.id.Call:
                    Uri uri= Uri.parse("tel:15700082120");
                    Intent intent=new Intent(Intent.ACTION_CALL,uri);
                    startActivity(intent);
                    break;
                case R.id.uploadAgain:
                    Bundle bundle=new Bundle();
                    UserBean user=new UserBean();
                    user.setAccount(account);
                    bundle.putParcelable("userInfo", user);
                    UserUploadPicFragment userUploadPicFragment=new UserUploadPicFragment();
                    userUploadPicFragment.setArguments(bundle);
                    changeFragment(userUploadPicFragment);
                    break;
            }
        }
    };

    private void changeFragment(Fragment targetFragment){
        resideMenu.clearIgnoredViewList();
        getFragmentManager()
                .beginTransaction()
                .replace(R.id.main_fragment, targetFragment, "fragment")
                .setTransitionStyle(FragmentTransaction.TRANSIT_FRAGMENT_FADE)
                .commit();
    }
}