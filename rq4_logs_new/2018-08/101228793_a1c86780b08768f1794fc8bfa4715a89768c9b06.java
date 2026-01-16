package io.fcmchannel.sdk.chat.metadata;

/**
 * Created by john-mac on 6/30/16.
 */
public interface OnMetadataItemClickListener {

    void onClickQuickReply(String reply);
    void onClickUrlButton(String url);

}