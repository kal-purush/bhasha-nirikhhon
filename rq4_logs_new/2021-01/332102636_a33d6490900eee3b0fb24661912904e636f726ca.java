package com.long51xy.flutter.plugin.flutter_payu;

import android.annotation.SuppressLint;
import android.app.Activity;
import android.content.Intent;
import android.text.TextUtils;
import android.util.Log;
import android.webkit.WebView;

import androidx.annotation.Nullable;

import com.payu.base.models.ErrorResponse;
import com.payu.base.models.PayUPaymentParams;
import com.payu.checkoutpro.PayUCheckoutPro;
import com.payu.checkoutpro.utils.PayUCheckoutProConstants;
import com.payu.ui.model.listeners.PayUCheckoutProListener;
import com.payu.ui.model.listeners.PayUHashGenerationListener;

import org.json.JSONException;
import org.json.JSONObject;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import io.flutter.plugin.common.MethodChannel.Result;
import io.flutter.plugin.common.PluginRegistry.ActivityResultListener;

public class FlutterPayuDelegate implements ActivityResultListener {
    private final Activity activity;
    private Result pendingResult;
    private Map<String, Object> pendingReply;

    public FlutterPayuDelegate(Activity activity) {
        this.activity = activity;
    }

    @Override
    public boolean onActivityResult(int requestCode, int resultCode, Intent data) {
        return false;
    }

    // 拉起支付
    @SuppressLint("LongLogTag")
    void doPayment(Map<String, Object> arguments, Result result) throws JSONException {
        this.pendingResult = result;

        JSONObject options = new JSONObject(arguments);
        String merchantKey = options.optString("merchantKey");
        if (isEmpty(merchantKey)) {
            Log.d("merchantKey is required", merchantKey);
            errorReply("merchantKey is required");
            return;
        }

        String merchantSalt = options.optString("merchantSalt");

        String amount = options.optString("amount");
        if (isEmpty(amount)) {
            Log.d("amount is required", amount);
            errorReply("amount is required");
            return;
        }

        String transactionId = options.optString("transactionId");
        if (isEmpty(transactionId)) {
            Log.d("transactionId is required", transactionId);
            errorReply("transactionId is required");
            return;
        }

        String productInfo = options.optString("productInfo");
        if (isEmpty(productInfo)) {
            Log.d("productInfo is required", productInfo);
            errorReply("productInfo is required");
            return;
        }

        String firstName = options.optString("firstName");
        if (isEmpty(firstName)) {
            Log.d("firstName is required", firstName);
            errorReply("firstName is required");
            return;
        }

        String email = options.optString("email");
        if (isEmpty(email)) {
            Log.d("email is required", firstName);
            errorReply("email is required");
            return;
        }

        String userCredential = options.optString("userCredential");

        String phone = options.optString("phone");
        String surl = options.optString("surl");
        if (isEmpty(surl)) {
            Log.d("surl is required", surl);
            errorReply("surl is required");
            return;
        }
        String furl = options.optString("furl");
        if (isEmpty(furl)) {
            Log.d("furl is required", furl);
            errorReply("furl is required");
            return;
        }
        boolean isProduction = options.optBoolean("isProduction", false);

        String udf1 = options.optString("udf1");
        String udf2 = options.optString("udf2");
        String udf3 = options.optString("udf3");
        String udf4 = options.optString("udf4");
        String udf5 = options.optString("udf5");

        String hash1String = options.optString("hash1");
        if (isEmpty(hash1String) && !isEmpty(merchantSalt)) {
            String hash1Data = merchantKey + "|payment_related_details_for_mobile_sdk|" + userCredential + "|" + merchantSalt;
            Log.i("hash1Data", hash1Data);
            hash1String = getSHA(hash1Data);
            Log.d("hash1", hash1String);
        }

        String hash2String = options.optString("hash2");
        if (isEmpty(hash2String) && !isEmpty(merchantSalt)) {
            String hash2Data = merchantKey + "|vas_for_mobile_sdk|default|" + merchantSalt;
            Log.d("hash2Data", hash2Data);
            hash2String = getSHA(hash2Data);
            Log.d("hash2", hash2String);
        }

        String hash3String = options.optString("hash3");
        if (isEmpty(hash3String) && !isEmpty(merchantSalt)) {
            String hash3Data = merchantKey + "|eligibleBinsForEMI|default|" + merchantSalt;
            Log.d("hash3Data", hash3Data);
            hash3String = getSHA(hash3Data);
            Log.d("hash3", hash3String);
        }

        String hash4String = options.optString("hash4");
        if (isEmpty(hash4String) && !isEmpty(merchantSalt)) {
            String hash4Data = merchantKey + "|vas_for_mobile_sdk|" + amount + "|" + merchantSalt;
            Log.d("hash3Data", hash4Data);
            hash4String = getSHA(hash4Data);
            Log.d("hash4", hash4String);
        }

        String hash5String = options.optString("hash5");
        if (isEmpty(hash5String) && !isEmpty(merchantSalt)) {
            String hash5Data = merchantKey + "|delete_user_card|" + userCredential + "|" + merchantSalt;
            Log.d("hash5Data", hash5Data);
            hash5String = getSHA(hash5Data);
            Log.d("hash5", hash5String);
        }

        String hash6String = options.optString("hash6");
        if (isEmpty(hash6String) && !isEmpty(merchantSalt)) {
            String hash6Data = merchantKey + "|" + transactionId + "|" + amount + "|" + productInfo + "|" + firstName + "|" + email + "|" + udf1 + "|" + udf2 + "|" + udf3 + "|" + udf4 + "|" + udf5 + "||||||" + merchantSalt;
            Log.d("hash6Data", hash6Data);
            hash6String = getSHA(hash6Data);
            Log.d("hash6", hash6String);
        }

        HashMap<String, Object> additionalParams = new HashMap<>();
        additionalParams.put(PayUCheckoutProConstants.CP_UDF1, udf1);
        additionalParams.put(PayUCheckoutProConstants.CP_UDF2, udf2);
        additionalParams.put(PayUCheckoutProConstants.CP_UDF3, udf3);
        additionalParams.put(PayUCheckoutProConstants.CP_UDF4, udf4);
        additionalParams.put(PayUCheckoutProConstants.CP_UDF5, udf5);
        additionalParams.put(PayUCheckoutProConstants.CP_PAYMENT_RELATED_DETAILS_FOR_MOBILE_SDK, hash1String);
        additionalParams.put(PayUCheckoutProConstants.CP_VAS_FOR_MOBILE_SDK, hash2String);
        additionalParams.put(PayUCheckoutProConstants.CP_ELIGIBLE_BINS_FOR_EMI, hash3String);
        additionalParams.put(PayUCheckoutProConstants.CP_GET_EMI_AMOUNT_ACCORDING_TO_INTEREST, hash4String);
        additionalParams.put(PayUCheckoutProConstants.CP_USER_CREDENTIAL, hash5String);
        additionalParams.put(PayUCheckoutProConstants.CP_PAYMENT_HASH, hash6String);

        PayUPaymentParams.Builder builder = new PayUPaymentParams.Builder();
        builder.setAmount(amount)
                .setIsProduction(isProduction)
                .setProductInfo(productInfo)
                .setKey(merchantKey)
                .setPhone(phone)
                .setTransactionId(transactionId)
                .setFirstName(firstName)
                .setEmail(email)
                .setSurl(surl)
                .setFurl(furl)
                .setUserCredential(userCredential)
                .setAdditionalParams(additionalParams);
        PayUPaymentParams payUPaymentParams = builder.build();

        // 拉起支付
        PayUCheckoutPro.open(activity, payUPaymentParams, payUCheckoutProListener());
    }

    private boolean isEmpty(Object object) {
        if (object == null) {
            return true;
        } else if (object instanceof String) {
            if ("null".equalsIgnoreCase((String) object)) {
                return true;
            } else {
                return ((String) object).isEmpty();
            }
        } else {
            return false;
        }
    }

    private String getSHA(String str) {
        MessageDigest md;
        String out = "";
        try {
            md = MessageDigest.getInstance("SHA-512");
            md.update(str.getBytes());
            byte[] mb = md.digest();

            for (int i = 0; i < mb.length; i++) {
                byte temp = mb[i];
                String s = Integer.toHexString(new Byte(temp));
                while (s.length() < 2) {
                    s = "0" + s;
                }
                s = s.substring(s.length() - 2);
                out += s;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return out;
    }

    private PayUCheckoutProListener payUCheckoutProListener() {
        return new PayUCheckoutProListener() {

            @Override
            public void onPaymentSuccess(Object response) {
                //Cast response object to HashMap
                HashMap<String, Object> result = (HashMap<String, Object>) response;
                String payuResponse = (String) result.get(PayUCheckoutProConstants.CP_PAYU_RESPONSE);
                String merchantResponse = (String) result.get(PayUCheckoutProConstants.CP_MERCHANT_RESPONSE);
                Map<String, Object> reply = new HashMap<>();
                Map<String, Object> data = new HashMap<>();
                data.put("payuResponse", payuResponse);
                data.put("merchantResponse", merchantResponse);
                reply.put("type", 3);
                reply.put("data", data);
                sendReply(reply);
            }

            @Override
            public void onPaymentFailure(Object response) {
                //Cast response object to HashMap
                HashMap<String, Object> result = (HashMap<String, Object>) response;
                String payuResponse = (String) result.get(PayUCheckoutProConstants.CP_PAYU_RESPONSE);
                String merchantResponse = (String) result.get(PayUCheckoutProConstants.CP_MERCHANT_RESPONSE);
                Map<String, Object> reply = new HashMap<>();
                Map<String, Object> data = new HashMap<>();
                data.put("payuResponse", payuResponse);
                data.put("merchantResponse", merchantResponse);
                reply.put("type", 2);
                reply.put("data", data);
                sendReply(reply);
            }

            @Override
            public void onPaymentCancel(boolean isTxnInitiated) {
                Map<String, Object> reply = new HashMap<>();
                reply.put("type", 1);
                sendReply(reply);
            }

            @Override
            public void onError(ErrorResponse errorResponse) {
                String errorMessage = errorResponse.getErrorMessage();
                errorReply(errorMessage);
            }

            @Override
            public void setWebViewProperties(@Nullable WebView webView, @Nullable Object o) {
                //For setting webview properties, if any. Check Customized Integration section for more details on this
            }

            @Override
            public void generateHash(HashMap<String, String> valueMap, PayUHashGenerationListener hashGenerationListener) {
                String hashName = valueMap.get(PayUCheckoutProConstants.CP_HASH_NAME);
                String hashData = valueMap.get(PayUCheckoutProConstants.CP_HASH_STRING);
                if (!TextUtils.isEmpty(hashName) && !TextUtils.isEmpty(hashData)) {
                    //Do not generate hash from local, it needs to be calculated from server side only. Here, hashString contains hash created from your server side.
//                    String hash = hashString;
//                    HashMap<String, String> dataMap = new HashMap<>();
//                    dataMap.put(hashName, hash);
//                    hashGenerationListener.onHashGenerated(dataMap);
                }
            }
        };
    }

    private void errorReply(String errorMessage) {
        Map<String, Object> reply = new HashMap<>();
        Map<String, Object> data = new HashMap<>();
        reply.put("type", 0);
        data.put("message", errorMessage);
        reply.put("data", data);
        sendReply(reply);
    }

    private void sendReply(Map<String, Object> data) {
        if (pendingResult != null) {
            pendingResult.success(data);
            pendingReply = null;
        } else {
            pendingReply = data;
        }
    }

    public void resync(Result result) {
        result.success(pendingReply);
        pendingReply = null;
    }
}