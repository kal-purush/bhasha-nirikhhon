package np.com.naxa.factsnepal.utils;

import android.annotation.SuppressLint;
import android.content.Context;
import android.graphics.Color;
import android.os.Handler;
import android.os.Message;
import android.support.annotation.Nullable;
import android.support.annotation.StyleableRes;
import android.text.Html;
import android.text.TextUtils;
import android.util.AttributeSet;
import android.util.Log;
import android.util.TypedValue;
import android.view.View;
import android.view.ViewGroup;
import android.widget.CheckBox;
import android.widget.LinearLayout;
import android.widget.RadioButton;
import android.widget.RadioGroup;
import android.widget.TextView;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JsonView extends LinearLayout {
    public interface RenderingCompleteListener {
        void onRendered(boolean isSuccess);
    }

    enum Type {
        CHECKBOX, RADIO, RATING
    }

    public static class ViewParams {
        String title;
        JSONArray options;
        Type type;
        int titleStyleRes = 0;
        int optionStyleRes = 0;

        static Map<String, Type> typeMap = new HashMap<String, Type>() {{
            put("checkbox", Type.CHECKBOX);
            put("radio", Type.RADIO);
            put("rating", Type.RATING);
        }};

        private ViewParams(String title, JSONArray options, Type type) {
            this.title = title;
            this.options = options;
            this.type = type;
        }

        public void addTitleStyle(int titleStyleId) {
            this.titleStyleRes= titleStyleId;
        }

        public void addOptionStyle(int optionStyleRes) {
            this.optionStyleRes = optionStyleRes;
        }
        public void addStyle(int titleStyleId, int optionStyleRes) {
            this.titleStyleRes = titleStyleId;
            this.optionStyleRes = optionStyleRes;
        }



        public static ViewParams instancefromString(String viewData) throws JSONException {
            try {
                return instanceFromJSON(new JSONObject(viewData));
            } catch (Exception e) {
                e.printStackTrace();
                throw new JSONException("Cannot render the view json \n view data : " + viewData);
            }
        }

        public static ViewParams instanceFromJSON(JSONObject jsonObject) throws JSONException {
            boolean isValidJSON = jsonObject.has("question")
                    && jsonObject.has("question_type")
                    && jsonObject.has("options");
            if (!isValidJSON) throw new JSONException("Invlaid json");

            String title = jsonObject.optString("question");
            String question_type = jsonObject.optString("question_type");
            JSONArray options = jsonObject.optJSONArray("options");
            return new ViewParams(title, options, typeMap.get(question_type));
        }
    }

    ViewParams params;
    Handler handler = null;
    BuildUi buildUi = null;
    RenderingCompleteListener listener = null;

    public JsonView(Context context) {
        super(context);
    }

    public JsonView(Context context, @Nullable AttributeSet attrs) {
        super(context, attrs);
    }

    public JsonView(Context context, @Nullable AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
    }

    public JsonView init(ViewParams params) {
        this.params = params;
        this.setOrientation(VERTICAL);
        return this;
    }

    public JsonView init(ViewParams params, RenderingCompleteListener listener) {
        this.params = params;
        this.listener = listener;
        this.setOrientation(VERTICAL);
        return this;
    }

    @SuppressLint("HandlerLeak")
    public void create() {
        if (params != null) {
            handler = new Handler() {
                @Override
                public void handleMessage(Message msg) {
                    super.handleMessage(msg);
                    if (listener != null) {
                        listener.onRendered(msg.what == 1);
                    }
                }
            };
            buildUi = new BuildUi();
            handler.post(buildUi);
        }
    }

    private synchronized ViewGroup getCheckBoxView() {
        LinearLayout chkGroup = new LinearLayout(getContext());
        chkGroup.setOrientation(VERTICAL);
        for (int i = 0; i < params.options.length(); i++) {
            JSONObject option = params.options.optJSONObject(i);
            CheckBox checkBox;
            if (params.optionStyleRes != 0) {
                checkBox = new CheckBox(getContext(), null, params.optionStyleRes);
            } else {
                checkBox = new CheckBox(getContext());
            }
            checkBox.setText(option.optString("question"));
            checkBox.setTag(option.optString("id"));
            chkGroup.addView(checkBox, i);
        }
        return chkGroup;
    }

    private synchronized RadioGroup getRadioGroupView() {
        RadioGroup radioGroup = new RadioGroup(getContext());
        radioGroup.setOrientation(VERTICAL);
        for (int i = 0; i < params.options.length(); i++) {
            JSONObject option = params.options.optJSONObject(i);
            RadioButton radioButton;
            if (params.optionStyleRes != 0) {
                radioButton = new RadioButton(getContext(), null, params.optionStyleRes);
            } else {
                radioButton = new RadioButton(getContext());
            }
            radioButton.setText(option.optString("question"));
            radioGroup.addView(radioButton);
            radioButton.setTag(option.optString("id"));
        }
        return radioGroup;
    }

    private synchronized TextView getTextView(String text) {
        TextView textView;
        Log.d("getText", "titleRes = " + params.titleStyleRes);
        if (params.titleStyleRes != 0) {
            textView= new TextView(getContext(), null, params.titleStyleRes);
        }else {
            textView= new TextView(getContext());
        }
//        textView.setTextAppearance(params.titleStyleRes);
        textView.setText(Html.fromHtml(text));
        textView.setTextColor(Color.parseColor("#212121"));
        textView.setTextSize(TypedValue.COMPLEX_UNIT_SP, 16);
        return textView;
    }

    void destroy() {
        if (handler != null && buildUi != null) {
            handler.removeCallbacks(buildUi);
        }
    }


    class BuildUi implements Runnable {
        @Override
        public void run() {
            try {
                if (!TextUtils.isEmpty(params.title)) {
                    JsonView.this.removeAllViews();
                    JsonView.this.addView(getTextView(params.title), 0);
                }
                if (params.options != null && params.options.length() > 0) {
                    ViewGroup optionGroup = null;
                    switch (params.type) {
                        case CHECKBOX:
                            Log.d("JsOnView", "in checkbox");
                            optionGroup = getCheckBoxView();
                            break;
                        case RADIO:
                            optionGroup = getRadioGroupView();
                            break;
                        case RATING:
                            break;
                    }
                    if (optionGroup != null) {
                        JsonView.this.addView(optionGroup, 1);
                        sendMessage(true);
                    } else {
                        sendMessage(false);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                sendMessage(false);
            }
        }

        private void sendMessage(boolean status) {
            Message message = new Message();
            message.what = status ? 0 : 1;
            handler.sendMessage(message);
        }
    }
}