package com.payline.payment.oney.exception;

import com.payline.pmapi.bean.common.FailureCause;
import com.payline.pmapi.bean.payment.response.impl.PaymentResponseFailure;
import com.payline.pmapi.bean.refund.response.impl.RefundResponseFailure;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DecryptExceptionTest {

    private DecryptException decryptException;

    @Test
    public void test() {
        DecryptException decryptException = new DecryptException(new Exception("test"), "origin");
        Assertions.assertEquals("origin", decryptException.getErrorCodeOrLabel());
        Assertions.assertEquals("test", decryptException.getMessage());
    }


    @Test
    void toPaymentResponseFailure() {
        decryptException = new DecryptException((Exception) null, "errorCodeOrLabelerrorCodeOrLabelerrorCodeOrLabelerrorCodeOrLabelerrorCodeOrLabel");

        PaymentResponseFailure paymentResponseFailure = decryptException.toPaymentResponseFailure();
        Assertions.assertEquals(FailureCause.PARTNER_UNKNOWN_ERROR, paymentResponseFailure.getFailureCause());
        Assertions.assertTrue(paymentResponseFailure.getErrorCode().contains("errorCodeOrLabel"));
        Assertions.assertEquals(50, paymentResponseFailure.getErrorCode().length());
    }

    @Test
    void toRefundResponseFailure() {
        decryptException = new DecryptException((Exception) null, "errorCodeOrLabelerrorCodeOrLabelerrorCodeOrLabelerrorCodeOrLabelerrorCodeOrLabel");

        RefundResponseFailure refundResponseFailure = decryptException.toRefundResponseFailure();
        Assertions.assertEquals(FailureCause.PARTNER_UNKNOWN_ERROR, refundResponseFailure.getFailureCause());
        Assertions.assertTrue(refundResponseFailure.getErrorCode().contains("errorCodeOrLabel"));
        Assertions.assertEquals(50, refundResponseFailure.getErrorCode().length());
    }
}