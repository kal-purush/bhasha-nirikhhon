package kr.ac.kpu.block.smared;

import android.content.Context;
import android.database.Cursor;
import android.net.Uri;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class SMSParser {
    public Ledger parseSingleSMS(String smsOriginalMessage, long smsReceiptDateTime) {
        Ledger ledger = new Ledger();

        // SMS 수신 시간 저장
        ledger.setPaymentTimestamp(smsReceiptDateTime);

        // 현재 신한 체크카드 메시지만 처리할 수 있다.
        if (!smsOriginalMessage.contains("신한체크승인")) {
            return ledger;
        }

        // SMS에서 결제 금액에서 숫자만을 파싱한다.
        StringTokenizer tokenizer = new StringTokenizer(smsOriginalMessage, " ");
        tokenizer.nextToken();
        tokenizer.nextToken();
        tokenizer.nextToken();
        tokenizer.nextToken();
        String smsPrice;
        smsPrice = tokenizer.nextToken().trim();
        smsPrice = smsPrice.replace(",","");
        smsPrice = smsPrice.replace("원","");
        ledger.setTotalPrice(Double.parseDouble(smsPrice));

        // SMS에서 결제 내역 상세를 파싱한다.
        String smsDescription = tokenizer.nextToken();
        ledger.setDescription(smsDescription);

        // SMS에서 가게명을 파싱한다.
        String smsStoreName = tokenizer.nextToken();
        ledger.setStoreName(smsStoreName);

        return ledger;
    }

    public List<Ledger> parseAllSMS(Context context) {
        List<Ledger> parsedLedgerData = new ArrayList<>();

        // 전체 SMS 받아오기
        Cursor smsCursor = context.getContentResolver().query(Uri.parse("content://sms/inbox"), null, null, null, null);
        if (smsCursor == null) {
            return parsedLedgerData;
        }

        while (smsCursor.moveToNext()) {
            int smsBodyIndex = smsCursor.getColumnIndex("body");
            int smsDateIndex = smsCursor.getColumnIndex("date");

            if (smsBodyIndex <= 0)
                continue;
            if (smsDateIndex <= 0)
                continue;

            String smsOriginalMessage = smsCursor.getString(smsBodyIndex);
            long smsReceiptDateTime = smsCursor.getLong(smsDateIndex);

            // SMS를 하나씩 파싱하여 리스트에 추가한다.
            Ledger ledger = parseSingleSMS(smsOriginalMessage, smsReceiptDateTime);
            parsedLedgerData.add(ledger);
        }

        return parsedLedgerData;
    }
}