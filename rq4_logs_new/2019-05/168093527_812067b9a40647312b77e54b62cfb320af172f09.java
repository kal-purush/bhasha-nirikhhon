package com.spring.boot.mybatis.demo.controller;

/**
 * @auther xuxq
 * @date 2019/2/19 10:09
 */
public class ShareBean {
    private static final int TYPE_LINK = 0;
    private static final int TYPE_IMAGE = 1;
    private static final int TYPE_TEXT = 2;
    private static final int TYPE_IMAGE_TEXT = 3;

    public class ShareItem {
        int type;
        String title;
        String content;
        String imagePath;
        String link;
    }

    public interface ShareListener {

        int STATE_SUCC = 0;
        int STATE_FAIL = 1;

        void onCallback(int state, String msg);
    }

//    public void share (ShareItem item, ShareListener listener) {
//        if (item != null) {
//            if (item.type == TYPE_LINK) {
//                // 分享链接
//                if (!TextUtils.isEmpty(item.link) && !TextUtils.isEmpty(item.title)) {
//                    doShareLink(item.link, item.title, item.content, listener);
//                } else {
//                    if (listener != null) {
//                        listener.onCallback(ShareListener.STATE_FAIL, "分享信息不完整");
//                    }
//                }
//            } else if (item.type == TYPE_IMAGE) {
//                // 分享图片
//                if (!TextUtils.isEmpty(item.imagePath)) {
//                    doShareImage(item.imagePath, listener);
//                } else {
//                    if (listener != null) {
//                        listener.onCallback(ShareListener.STATE_FAIL, "分享信息不完整");
//                    }
//                }
//            } else if (item.type == TYPE_TEXT) {
//                // 分享文本
//                if (!TextUtils.isEmpty(item.content)) {
//                    doShareText(item.content, listener);
//                } else {
//                    if (listener != null) {
//                        listener.onCallback(ShareListener.STATE_FAIL, "分享信息不完整");
//                    }
//                }
//            } else if (item.type == TYPE_IMAGE_TEXT) {
//                // 分享图文
//                if (!TextUtils.isEmpty(item.imagePath) && !TextUtils.isEmpty(item.content)) {
//                    doShareImageAndText(item.imagePath, item.content, listener);
//                } else {
//                    if (listener != null) {
//                        listener.onCallback(ShareListener.STATE_FAIL, "分享信息不完整");
//                    }
//                }
//            } else {
//                if (listener != null) {
//                    listener.onCallback(ShareListener.STATE_FAIL, "不支持的分享类型");
//                }
//            }
//        } else {
//            if (listener != null) {
//                listener.onCallback(ShareListener.STATE_FAIL, "ShareItem 不能为 null");
//            }
//        }
//    }
}