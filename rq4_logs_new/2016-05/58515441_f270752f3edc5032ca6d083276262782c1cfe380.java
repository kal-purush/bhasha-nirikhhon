package au.appxperts.ga.mapsurvey.progressdialog;

/**
 * Created by xpertsinfosoftmac-3 on 20/05/16.
 */
import android.app.Dialog;
import android.content.Context;
import android.view.Gravity;
import android.view.WindowManager;
import android.widget.TextView;

import com.github.lzyzsd.circleprogress.DonutProgress;

import au.appxperts.ga.mapsurvey.R;


public class ProgressDialog extends Dialog {


    public TextView message;
    public DonutProgress fileProgress;

    public ProgressDialog(Context context) {
        super(context);


    }

    public ProgressDialog(Context context, int theme) {
        super(context, theme);
    }


    public static ProgressDialog show(Context context) {
        if (context == null) {
            return null;
        }

        ProgressDialog dialog = new ProgressDialog(context, R.style.ProgressHUD);
        dialog.setTitle("");
        dialog.setContentView(R.layout.progressdialog);
        dialog.message = (TextView)dialog.findViewById(R.id.message);
        dialog.fileProgress = (DonutProgress)dialog.findViewById(R.id.donut_progress);
        dialog.setCancelable(false);
        dialog.getWindow().getAttributes().gravity = Gravity.CENTER;
        WindowManager.LayoutParams lp = dialog.getWindow().getAttributes();
        lp.dimAmount = 0.5f;
        dialog.getWindow().setAttributes(lp);
        dialog.show();
        return dialog;
    }

    public void setMessage(String message) {
        if(this.message!=null)
        this.message.setText(message);
    }

    public void setProgress(int progress) {
        if(this.fileProgress!=null)
            this.fileProgress.setProgress(progress);
    }
}