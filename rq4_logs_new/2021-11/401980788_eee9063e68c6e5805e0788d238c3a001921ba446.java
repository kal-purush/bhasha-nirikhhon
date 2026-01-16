package com.diem;

import static org.junit.Assert.assertEquals;

import com.diem.jsonrpc.DiemJsonRpcClient;
import com.diem.jsonrpc.JsonRpc;
import com.diem.types.AccountState;
import com.diem.types.ChainId;
import com.diem.utils.Hex;
import com.novi.bcs.BcsDeserializer;
import com.novi.serde.Deserializer;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AccountStateTest {
    final String JSON_RPC_URL = "http://124.251.110.220:50001";
    final ChainId CHAIN_ID = new ChainId((byte) 4);

    public static class ExchangeRate {
        public String currency;
        public double  rate;

        public ExchangeRate() {
            currency = "";
            rate = 1;
        }
    }

    @Test
    public void testDeserialize( ) throws Exception {
        List<ExchangeRate> rates = new ArrayList<>();
        final String oracleAdminAccountAddress = "0000000000000000000000004f524143";
        DiemJsonRpcClient mClient = new DiemJsonRpcClient(JSON_RPC_URL, CHAIN_ID);

        JsonRpc.AccountStateWithProof stateWithProof = mClient.getAccountStateWithProof(oracleAdminAccountAddress, 0, 0);
        String blob = stateWithProof.getBlob();

        assertEquals(blob.isEmpty(), false);

        try {
            Deserializer des = new BcsDeserializer(Hex.decode(blob));
            AccountState accountState = AccountState.deserialize(des);

            ExchangeRate rate = accountState.getResource(new byte[32], ExchangeRate.class );
            rate.currency = new String("test");

        }catch (Exception e) {
            System.console().printf("%s", e.getMessage());
        }


    }
}