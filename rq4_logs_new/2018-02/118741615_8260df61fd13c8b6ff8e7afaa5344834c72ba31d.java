package com.app.impl;

import com.app.constant.Constant;
import com.app.interfaces.EmailSendService;
import com.app.interfaces.ShareInfoEmailService;
import com.app.model.ShareInfo;
import com.app.model.ShareSetting;
import com.app.util.HttpUtil;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.tomcat.util.http.FastHttpDateFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @author 莫小阳
 */
@Slf4j
@Service
public class ShareInfoEmailServiceImpl implements ShareInfoEmailService {


    /**
     * 股票信息配置
     */
    @Autowired
    private ShareSetting shareSetting;

    /**
     * 发送邮件服务
     */
    @Autowired
    private EmailSendService emailSendService;

    /**
     * 发送股票信息邮件
     *
     * @param traceLogId 日志ID
     */
    @Override
    public void sendShareInfoEmail(String traceLogId) {
        String content = HttpUtil.getContentByGet(shareSetting.getUrl(), Constant.GBK);
        content = content.substring(content.indexOf("[") + 2, content.indexOf("]") - 1);
        String[] split = content.split("\",\"");
        List<String> list = Arrays.asList(split);
        List<ShareInfo> shareInfoList = Lists.newArrayList();
        for (String s : list) {
            String[] split1 = s.split(",");
            if (shareSetting.getCode().contains(split1[1])) {
                ShareInfo shareInfo = new ShareInfo();
                shareInfo.setCode(split1[1]);
                shareInfo.setShareName(split1[2]);
                shareInfo.setPrice(split1[3]);
                shareInfo.setValue(Double.valueOf(split1[4]));
                shareInfoList.add(shareInfo);
            }
        }

        if (shareInfoList.isEmpty()) {
            log.error("发送信息为空!");
            return;
        }

        doSend(shareInfoList);

    }

    /**
     * 发送信息
     *
     * @param shareInfoList 股票信息
     */
    private void doSend(List<ShareInfo> shareInfoList) {

        Collections.sort(shareInfoList, (o1, o2) -> {
            if (o1.getValue() > o2.getValue()) {
                return 1;
            } else {
                return 0;
            }
        });

        String format = FastDateFormat.getInstance(Constant.YYYYMMDDHHMMSS).format(new Date());
        String content = Constant.SHARE_HTML.replace("${date}", format);
        String tr = "";
        for (ShareInfo shareInfo : shareInfoList) {
            tr = tr.concat(Constant.SHARE_TR.replace("${code}", shareInfo.getCode()).replace("${name}", shareInfo.getShareName()).
                    replace("${price}", shareInfo.getPrice()).replace("${value}", String.valueOf(shareInfo.getValue())));
        }
        content = content.replace("${info}", tr);
        emailSendService.sendEmailHtml(content, shareSetting.getMailAddressTO(),
                null, null, null, "股票价格信息",
                null, null, null, "王东江");
    }
}