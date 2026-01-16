package com.renshihan.settlement.bean;

import com.renshihan.settlement.utils.AmountUtil;
import lombok.Data;
import lombok.experimental.Accessors;

import java.math.BigDecimal;

@Data
@Accessors(chain = true)
public class LevelTwoPriceFlowDto {
    private Long id;
    private Long userId;
    private BigDecimal viewPrice;
    private BigDecimal payPrice;
    private BigDecimal lossPrice;
    @Override
    public String toString(){
        String str_format="%s %s %s %s"; //用户uid 看到金额 实际支付金额 需赔偿金额
        return String.format(
                str_format,
                this.userId,
                AmountUtil.rvZeroAndDot(this.viewPrice),
                AmountUtil.rvZeroAndDot(this.payPrice),
                AmountUtil.rvZeroAndDot(this.lossPrice)
        );
    }

}