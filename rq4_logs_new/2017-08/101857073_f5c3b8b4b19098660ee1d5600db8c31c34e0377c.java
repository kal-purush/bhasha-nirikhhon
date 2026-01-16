import com.iflytek.cloud.speech.*;

/**
 * Created by Administrator on 2017/8/30.
 */
public class ToText {
    private boolean flag=true;
    private SpeechRecognizer mIat;
    RecognizerListener recognizerListener;
    private StringBuffer text;
    public ToText(){
        SpeechUtility.createUtility(SpeechConstant.APPID+"=59a621b6");
        mIat=SpeechRecognizer.createRecognizer();
        mIat.setParameter(SpeechConstant.DOMAIN,"iat");
        mIat.setParameter(SpeechConstant.LANGUAGE,"zh_cn");
        mIat.setParameter(SpeechConstant.ACCENT,"mandarin");
        mIat.setParameter(SpeechConstant.RESULT_TYPE,"plain");
        recognizerListener=new RecognizerListener() {
            @Override
            public void onVolumeChanged(int i) {
                System.out.println("onVolumeChanged-------->"+i);
            }

            @Override
            public void onBeginOfSpeech() {
                text=new StringBuffer();
            }

            @Override
            public void onEndOfSpeech() {
                System.out.println("onEndOfSpeech");
            }

            @Override
            public void onResult(RecognizerResult recognizerResult, boolean b) {
                text.append(recognizerResult.toString());
            }

            @Override
            public void onError(SpeechError speechError) {
                System.out.print(speechError.getErrorDescription(true));
            }

            @Override
            public void onEvent(int i, int i1, int i2, String s) {

            }
        };
    }

    public String getText(){
        return text.toString();
    }

    public void stopListening(){
        if (mIat.isListening()){
            mIat.startListening(recognizerListener);
        }
    }

    public void startListening(){
        if (!mIat.isListening()){
            mIat.startListening(recognizerListener);
        }
    }
}