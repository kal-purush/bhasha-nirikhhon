import com.kbfoo.openapi.sdk.DefaultMD5OpenApiClient;
import com.kbfoo.openapi.sdk.MD5OpenApiClient;
import com.kbfoo.openapi.sdk.request.KbfooOpenApiAliH5PayRequest;
import com.kbfoo.openapi.sdk.response.MD5OpenApiResponse;

/**
 * Created by LL on 2017/11/5.
 */
public class TestMD5Pay {
    public static void main(String[] args) throws Exception {
        String MD5_KEY = "78d4b5b76fceea52b27929fe153bdd3a";
        MD5OpenApiClient MD5OpenApiClient = new DefaultMD5OpenApiClient(MD5_KEY);
        KbfooOpenApiAliH5PayRequest request = new KbfooOpenApiAliH5PayRequest();
        request.setPartner("101520216381");//聚合平台商户号
        request.setAppSn("app123");//商户应用编码
        request.setService("ali_h5");//service名称，固定为ali_h5
        request.setOutTradeNo("20161234");//商户订单号
        request.setAmount("1");//交易金额，单位为分
        request.setSubject("LL TEST");//商品名称
        request.setSubBody("LL TEST");//商品描述
        request.setRemark("LL TEST");//备注
        request.setReturnUrl("http://www.baidu.com/");//订单回调地址，需外网可访问
        String serviceUrl = "https://pay.swiftpass.cn/pay/gateway";//h5服务请求地址
        MD5OpenApiResponse OpenApiResponse = MD5OpenApiClient.excute(request, serviceUrl);
        System.out.println(OpenApiResponse);

    }
}