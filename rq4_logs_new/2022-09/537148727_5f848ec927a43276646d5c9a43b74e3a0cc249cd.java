package modules.controller;


import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.github.wxpay.sdk.WXPayConstants;
import com.github.wxpay.sdk.WXPayUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.log4j.Log4j2;
import modules.dto.ClassroomDto;
import modules.dto.ServiceWeiXinPayDTO;
import modules.entity.Classroom;
import modules.mapper.ClassroomMapper;
import modules.util.WeiXinPay.CommonUtil;
import modules.util.WeiXinPay.ConfigUtil;
import modules.vo.Result;
import org.springframework.beans.BeanUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.text.DecimalFormat;
import java.util.*;

/**
 * <p>
 *  微信支付 前端控制器
 * </p>
 *
 * @author kangshizhu
 * @since 2022-09-15
 */
@Api(tags = "微信支付接口")
@Log4j2
@RestController
@RequestMapping("/weiXinPay")
public class WeiXinPayController {

    private static final DecimalFormat df = new DecimalFormat("#");

    /**
     * 生成云服务微信支付二维码
     *
     *
     */
    @ApiOperation(value = "生成二维码 ", notes = "生成二维码")
    @PostMapping(value = "/ServiceCreateNative")
    @ResponseBody
    public Result ServiceCreateNative(@RequestBody @Valid ServiceWeiXinPayDTO serviceWeiXinPayDTO,
                                      HttpServletRequest request, HttpServletResponse response) {
        System.out.println("11111111");
        try {
            //生成唯一id 随机生成商户单号
            String id= UUID.randomUUID().toString();
            //替换uuid中的"-"
            id=id.replace("-", "");
            //todo 创建请求参数
            SortedMap<String, String> req = new TreeMap<String, String>();
            req.put("appid", ConfigUtil.APPID);    //公众号
            req.put("mch_id", ConfigUtil.MCH_ID);  // 商户号
            req.put("nonce_str", WXPayUtil.generateNonceStr()); // 32位随机字符串
            req.put("body", serviceWeiXinPayDTO.getDescription()); // 商品描述
            req.put("out_trade_no", id);   // 商户订单号
            req.put("total_fee", df.format(Double.parseDouble(serviceWeiXinPayDTO.getTotalFee()) * 100));    // 标价金额(分)
            req.put("spbill_create_ip", CommonUtil.getIp(request));   // 终端IP
            req.put("notify_url", "http://www.baidu.com");  // 回调地址
            req.put("trade_type", "NATIVE");    // 交易类型
            req.put("sign", WXPayUtil.generateSignature(req, ConfigUtil.API_KEY, WXPayConstants.SignType.MD5));  // 签名
            //todo 生成要发送的 xml
            String xmlBody = WXPayUtil.generateSignedXml(req, ConfigUtil.API_KEY);
            System.err.println(String.format("微信支付预下单请求 xml 格式:\n%s", xmlBody));
            //todo 发送 POST 请求 统一下单 API 并携带 xmlBody 内容,然后获得返回接口结果
            String result = CommonUtil.httpsRequest("https://api.mch.weixin.qq.com/pay/unifiedorder", "POST", xmlBody);
            System.err.println(String.format("%s", result));
            //todo 将返回结果从 xml 格式转换为 map 格式
            Map<String, String> resultMap = WXPayUtil.xmlToMap(result);
            Map<String, String> map = new HashMap<>();
            map.put("code_url", resultMap.get("code_url")); // 支付地址
            map.put("total_fee",serviceWeiXinPayDTO.getTotalFee()); // 总金额
            map.put("out_trade_no", id);// 订单号
            return Result.OK(map);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}