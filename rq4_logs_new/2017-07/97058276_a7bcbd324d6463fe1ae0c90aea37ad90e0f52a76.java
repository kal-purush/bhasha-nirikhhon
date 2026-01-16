package com.example.deadstrike.contactbook.adapters;

import android.support.design.widget.TextInputLayout;
import android.support.v7.widget.RecyclerView;
import android.telephony.PhoneNumberFormattingTextWatcher;
import android.text.Editable;
import android.text.InputType;
import android.text.TextWatcher;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageButton;

import com.example.deadstrike.contactbook.R;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContactInfoAdapter extends RecyclerView.Adapter<RecyclerView.ViewHolder> {

    public static final int TYPE_PHONE = 11;
    public static final int TYPE_EMAIL = 21;

    private List<String> mDataSet;
    private int mType;
    private boolean hasErrorInput = false;

    public ContactInfoAdapter(List<String> items, int viewType) {
        this.mDataSet = items;
        try {
            if (viewType == TYPE_EMAIL || viewType == TYPE_PHONE) {
                mType = viewType;
            } else throw new IllegalArgumentException();
        } catch (IllegalArgumentException ex) {
            Log.e("ContactInfoAdapter: ", "illegal view type argument");
        }
    }

    @Override
    public int getItemViewType(int position) {
        return mType;
    }

    @Override
    public RecyclerView.ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View itemView = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.single_input_with_del_button, parent, false);
        switch (viewType) {
            case TYPE_PHONE:
                return new InputPhoneViewHolder(itemView);
            case TYPE_EMAIL:
                return new InputEmailViewHolder(itemView);
            default:
                //TODO it can be a problem. FIX IT!
                return null;
        }
    }

    @Override
    public void onBindViewHolder(RecyclerView.ViewHolder vh, int position) {
        String info = mDataSet.get(position);
        switch (vh.getItemViewType()) {
            case TYPE_PHONE:
                InputPhoneViewHolder phoneViewHolder = (InputPhoneViewHolder) vh;
                phoneViewHolder.inputLayout.getEditText().setText(info);
                break;
            case TYPE_EMAIL:
                InputEmailViewHolder emailViewHolder = (InputEmailViewHolder) vh;
                emailViewHolder.inputLayout.getEditText().setText(info);
                break;
        }
    }

    @Override
    public int getItemCount() {
        return mDataSet.size();
    }

    public boolean hasErrorInput() {
        return hasErrorInput;
    }

    private void removeAt(int position) {
        mDataSet.remove(position);
        notifyItemRemoved(position);
        notifyItemRangeChanged(position, mDataSet.size());
    }

    private class InputPhoneViewHolder extends RecyclerView.ViewHolder {
        TextInputLayout inputLayout;
        ImageButton buttonDel;

        InputPhoneViewHolder(final View view) {
            super(view);

            inputLayout = (TextInputLayout) view.findViewById(R.id.input);
            inputLayout.getEditText().setInputType(
                    InputType.TYPE_CLASS_PHONE);
            inputLayout.getEditText().addTextChangedListener(new PhoneNumberFormattingTextWatcher());
            inputLayout.getEditText().addTextChangedListener(new TextWatcher() {
                @Override
                public void beforeTextChanged(CharSequence s, int start, int count, int after) {

                }

                @Override
                public void onTextChanged(CharSequence s, int start, int before, int count) {

                }

                @Override
                public void afterTextChanged(Editable s) {
                    mDataSet.set(getAdapterPosition(), s.toString());
                }
            });

            buttonDel = (ImageButton) view.findViewById(R.id.button_delete_item);
            buttonDel.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    view.clearFocus();
                    removeAt(getAdapterPosition());
                }
            });
        }
    }

    private class InputEmailViewHolder extends RecyclerView.ViewHolder {
        //regexp pattern to validate email address
        private static final String EMAIL_PATTERN = "^[a-zA-Z0-9#_~!$&'()*+,;=:.\"(),:;<>@\\[\\]\\\\]+@[a-zA-Z0-9-]+(\\.[a-zA-Z0-9-]+)*$";
        TextInputLayout inputLayout;
        ImageButton buttonDel;
        private Pattern pattern = Pattern.compile(EMAIL_PATTERN);

        InputEmailViewHolder(final View view) {
            super(view);

            inputLayout = (TextInputLayout) view.findViewById(R.id.input);
            inputLayout.getEditText().setInputType(
                    InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_VARIATION_EMAIL_ADDRESS);
            final String errorString = view.getResources().getString(R.string.invalid_email);
            inputLayout.getEditText().addTextChangedListener(new TextWatcher() {
                @Override
                public void beforeTextChanged(CharSequence s, int start, int count, int after) {
                }

                @Override
                public void onTextChanged(CharSequence s, int start, int before, int count) {
                }

                @Override
                public void afterTextChanged(Editable s) {
                    if (!validateEmail(s.toString())) {
                        hasErrorInput = true;
                        inputLayout.setErrorEnabled(true);
                        inputLayout.setError(errorString);
                    } else {
                        hasErrorInput = false;
                        inputLayout.setErrorEnabled(false);
                        inputLayout.setError(null);
                        mDataSet.set(getAdapterPosition(), s.toString());
                    }
                }
            });

            buttonDel = (ImageButton) view.findViewById(R.id.button_delete_item);
            buttonDel.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    view.clearFocus();
                    removeAt(getAdapterPosition());
                }
            });
        }

        private boolean validateEmail(String email) {
            if (email.equals(""))
                return true;
            Matcher matcher = pattern.matcher(email);
            return matcher.matches();
        }
    }
}