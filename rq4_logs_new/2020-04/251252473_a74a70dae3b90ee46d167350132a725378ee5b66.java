package salerecord;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sale.SaleRecord;
import sale.SaleRecordDtl;
import utils.DataConnection;
import utils.DateUtil;

import java.math.BigDecimal;
import java.sql.Connection;

public class TestSaleRecordMain {
  Connection appConn = null;

  @Before
  public void setUp() throws Exception {
    System.setProperty("APP_ENV", "test");
    appConn = DataConnection.getInstance().getAppConn();
    System.out.println("app connection");
    appConn.setAutoCommit(false);
  }

  @After
  public void tearDown() throws Exception {
    appConn.rollback();
    System.out.println("db rollback");
    appConn.setAutoCommit(true);
    DataConnection.getInstance().close();
    System.out.println("db connection closed.");
  }

  @Test
  public void saleRecordTest() throws Exception {

    SaleRecord saleRecord = new SaleRecord();
    saleRecord.setOrderId(1L);
    saleRecord.setRefundId(334455L);
    saleRecord.setChannelId(null);
    saleRecord.setStoreId(3L);
    saleRecord.setOuterOrderNo("1234567");
    saleRecord.setTenantCode("company1");
    saleRecord.setTotalListPrice(new BigDecimal("100.00"));
    saleRecord.setTotalDiscountPrice(new BigDecimal("20.00"));
    saleRecord.setTotalPaymentPrice(new BigDecimal("80.00"));
    saleRecord.setFreightPrice(new BigDecimal("6.00"));
    saleRecord.setChannelType("C");
    saleRecord.setCreatedId(1L);
    saleRecord.setSalesmanId(2L);
    saleRecord.setTransactionType("S");
    saleRecord.setOfflineShopCode("T01");
    saleRecord.setTradeCreated(DateUtil.parseDate("2020-01-01 08:00:00"));
    saleRecord.setSendStatus("P");

    SaleRecordDtl saleRecordDtl = new SaleRecordDtl();
    saleRecordDtl.setOrderItemId(4L);
    saleRecordDtl.setRefundItemId(55555L);
    saleRecordDtl.setBrandId(6L);
    saleRecordDtl.setBrandCode("NIKE");
    saleRecordDtl.setProductId(10010010L);
    saleRecordDtl.setSkuId(9000L);
    saleRecordDtl.setFeeRate(new BigDecimal("5.00"));
    saleRecordDtl.setListPrice(new BigDecimal("100.00"));
    saleRecordDtl.setSalePrice(new BigDecimal("100.00"));
    saleRecordDtl.setQuantity(1);
    saleRecordDtl.setTotalListPrice(new BigDecimal("100.00"));
    saleRecordDtl.setTotalDiscountPrice(new BigDecimal("20.00"));
    saleRecordDtl.setTotalPaymentPrice(new BigDecimal("80.00"));
    saleRecordDtl.setTid(9876543210L);
    saleRecordDtl.setOid(9876543211L);
    saleRecordDtl.setRefundId(11111L);
    saleRecordDtl.setTradeCreated(DateUtil.parseDate("2020-01-01 08:00:00"));

    saleRecord.addSaleRecordDtl(saleRecordDtl);
    Long saleRecordId = saleRecord.save();

    SaleRecord genSaleRecord = SaleRecord.fetchById(saleRecordId);
    //Assert SaleRecord
    Assert.assertEquals(saleRecord.getOrderId(), genSaleRecord.getOrderId());
    Assert.assertEquals(saleRecord.getRefundId(), genSaleRecord.getRefundId());
    Assert.assertEquals(saleRecord.getChannelId(), genSaleRecord.getChannelId());
    Assert.assertEquals(saleRecord.getStoreId(), genSaleRecord.getStoreId());
    Assert.assertEquals(saleRecord.getOuterOrderNo(), genSaleRecord.getOuterOrderNo());
    Assert.assertEquals(saleRecord.getTenantCode(), genSaleRecord.getTenantCode());
    Assert.assertEquals(saleRecord.getTotalListPrice(), genSaleRecord.getTotalListPrice());
    Assert.assertEquals(saleRecord.getTotalDiscountPrice(), genSaleRecord.getTotalDiscountPrice());
    Assert.assertEquals(saleRecord.getTotalPaymentPrice(), genSaleRecord.getTotalPaymentPrice());
    Assert.assertEquals(saleRecord.getFreightPrice(), genSaleRecord.getFreightPrice());
    Assert.assertEquals(saleRecord.getChannelType(), genSaleRecord.getChannelType());
    Assert.assertEquals(saleRecord.getCreatedId(), genSaleRecord.getCreatedId());
    Assert.assertEquals(saleRecord.getSalesmanId(), genSaleRecord.getSalesmanId());
    Assert.assertEquals(saleRecord.getTransactionType(), genSaleRecord.getTransactionType());
    Assert.assertEquals(saleRecord.getOfflineShopCode(), genSaleRecord.getOfflineShopCode());
    Assert.assertEquals(saleRecord.getTradeCreated(), genSaleRecord.getTradeCreated());
    Assert.assertEquals(saleRecord.getSendStatus(), genSaleRecord.getSendStatus());

    // assert sale record dtl
    SaleRecordDtl expectedDtl = saleRecord.getSaleRecordDtls().get(0);
    SaleRecordDtl actualDtl = genSaleRecord.getSaleRecordDtls().get(0);

    Assert.assertEquals(expectedDtl.getOrderItemId(), actualDtl.getOrderItemId());
    Assert.assertEquals(expectedDtl.getRefundItemId(), actualDtl.getRefundItemId());
    Assert.assertEquals(expectedDtl.getBrandId(), actualDtl.getBrandId());
    Assert.assertEquals(expectedDtl.getBrandCode(), actualDtl.getBrandCode());
    Assert.assertEquals(expectedDtl.getProductId(), actualDtl.getProductId());
    Assert.assertEquals(expectedDtl.getSkuId(), actualDtl.getSkuId());
    Assert.assertEquals(expectedDtl.getFeeRate(), actualDtl.getFeeRate());
    Assert.assertEquals(expectedDtl.getListPrice(), actualDtl.getListPrice());
    Assert.assertEquals(expectedDtl.getSalePrice(), actualDtl.getSalePrice());
    Assert.assertEquals(expectedDtl.getQuantity(), actualDtl.getQuantity());
    Assert.assertEquals(expectedDtl.getTotalListPrice(), actualDtl.getTotalListPrice());
    Assert.assertEquals(expectedDtl.getTotalDiscountPrice(), actualDtl.getTotalDiscountPrice());
    Assert.assertEquals(expectedDtl.getTotalPaymentPrice(), actualDtl.getTotalPaymentPrice());
    Assert.assertEquals(expectedDtl.getTid(), actualDtl.getTid());
    Assert.assertEquals(expectedDtl.getOid(), actualDtl.getOid());
    Assert.assertEquals(expectedDtl.getRefundId(), actualDtl.getRefundId());
    Assert.assertEquals(expectedDtl.getTradeCreated(), actualDtl.getTradeCreated());
  }
}