package jp.dev.atl.marubatsu;

import android.app.Activity;
import android.app.Dialog;
import android.view.View;
import android.view.WindowManager;
import android.view.animation.AnimationSet;
import android.view.animation.RotateAnimation;
import android.view.animation.ScaleAnimation;
import android.widget.ImageView;

/**
 * Created by oshita on 2014/12/13.
 */
public class Directage{

    /**
     * 勝利時のアニメーションを表示します。
     * @param context
     * @param playerType
     */
    public void showWinAnimation(Activity context,PlayerType playerType){

        Dialog dialog = new Dialog(context);
        dialog.setContentView(R.layout.anime_dialog);
        int dialogWidth = 1000;
        dialog.findViewById((R.id.draw)).setVisibility(View.GONE);
        ImageView img = (ImageView)dialog.findViewById((R.id.winImage2));
        RotateAnimation rotate = new RotateAnimation(0, 360, 30, 90);
        //動作時間の設定（単位ms）
        rotate.setDuration(1000);
        //繰り返す回数の設定（繰り返さない場合は不要）
        //rotate.setInterpolator(new CycleInterpolator(3));
        //アニメーションの開始
        img.startAnimation(rotate);

        WindowManager.LayoutParams lp = dialog.getWindow().getAttributes();
        lp.width = dialogWidth;
        lp.height = dialogWidth;
        dialog.getWindow().setAttributes(lp);

        if(playerType == PlayerType.Player1){
            dialog.setTitle("Player1  Win!");
        }else{
            dialog.setTitle("Player2  Win!");
        }

        dialog.show();
        return;
    }

    /**
     *  引き分け時のアニメーションを表示します。
     * @param context
     * @param playerType
     */
    public void showDrawAnimation(Activity context,PlayerType playerType){

        Dialog dialog = new Dialog(context);
        dialog.setContentView(R.layout.anime_dialog);
        int dialogWidth = 1000;
        dialog.findViewById((R.id.winImage2)).setVisibility(View.GONE);
        ImageView img = (ImageView)dialog.findViewById((R.id.draw));
        RotateAnimation rotate = new RotateAnimation(0, 360, 400, 400);
        //動作時間の設定（単位ms）
        rotate.setDuration(3000);
        //繰り返す回数の設定（繰り返さない場合は不要）
        //rotate.setInterpolator(new CycleInterpolator(3));
        ScaleAnimation scale = new ScaleAnimation(1,2,1,2, 300, 100);
        //動作時間の設定（単位ms）
        scale.setDuration(1500);

        AnimationSet set = new AnimationSet(true);
        set.addAnimation(rotate);
        set.addAnimation(scale);
        //アニメーションの開始
        img.startAnimation(set);
        WindowManager.LayoutParams lp = dialog.getWindow().getAttributes();
        lp.width = dialogWidth;
        lp.height = dialogWidth;
        dialog.getWindow().setAttributes(lp);

        dialog.setTitle("Draw Match!");

        dialog.show();
        return;
    }

}