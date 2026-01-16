package com.example.testingcodecountdown;


import android.content.Context;
import android.os.CountDownTimer;
import android.widget.Button;



/**
 * ��֤�뵹��ʱ
 * @author oceangray          , �����пؼ���չ
 *
 */
public class CaptchaTimeCount extends CountDownTimer{
	private Button validate_btn;
	private Context context;
	/**
	 * ��֤�뵹��ʱ
	 * @param millisInFuture
	 * @param countDownInterval
	 * @param validate_btn ����İ�ť
	 * @param context
	 */
	public CaptchaTimeCount(long millisInFuture, long countDownInterval,Button validate_btn,Context context) {
	    super(millisInFuture, countDownInterval);
	    this.validate_btn=validate_btn;
	    this.context=context;
	}
	@Override //��ʼ
	public void onTick(long millisUntilFinished) {
	    validate_btn.setClickable(false);
	    validate_btn.setText(millisUntilFinished / 1000 + "s���ط�");//  ��Ϊ  60�뵹��ʱ
	    validate_btn.setTextColor(context.getResources().getColorStateList(R.color.color_ffffff)); //��ɫ
	    validate_btn.setBackgroundResource(R.drawable.btn_captcha_unclickable);//������ɫ
	}
	@Override
	public void onFinish() {
	    validate_btn.setClickable(true); 
	    validate_btn.setText(R.string.captcha_btn_resend);//@color/color_63b953
	    validate_btn.setTextColor(context.getResources().getColorStateList(R.color.nav_bg_color));
	    validate_btn.setBackgroundResource(R.drawable.btn_captcha);
	}
	/**
	 * ����
	 */
	public void reset(){
		 validate_btn.setClickable(true);
		 validate_btn.setText(R.string.validate_btn_hint);
		 validate_btn.setTextColor(context.getResources().getColorStateList(R.color.nav_bg_color));
		 validate_btn.setBackgroundResource(R.drawable.btn_captcha);
	}
}