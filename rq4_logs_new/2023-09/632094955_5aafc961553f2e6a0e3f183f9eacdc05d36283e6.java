package com.artiexh.api.utils;

import org.apache.commons.codec.digest.HmacAlgorithms;
import org.apache.commons.codec.digest.HmacUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

public class PaymentUtils {
	private PaymentUtils() {}
	public static String generatePaymentUrl(
		String vnp_Version,
		String vnp_Command,
		String vnp_OrderInfo,
		String vnp_TxnRef,
		String vnp_IpAddr,
		String vnp_TmnCode,
		String vnp_ReturnUrl,
		String amount,
		String vnp_CurrCode,
		String vnp_Locale,
		String vnp_PaymentViewUrl,
		String vnp_SecretHash) {
		Map<String, String> vnp_Params = new HashMap<>();
		vnp_Params.put("vnp_Version", vnp_Version);
		vnp_Params.put("vnp_Command", vnp_Command);
		vnp_Params.put("vnp_TmnCode", vnp_TmnCode);
		vnp_Params.put("vnp_Amount", amount);
		vnp_Params.put("vnp_CurrCode", vnp_CurrCode);
		//vnp_Params.put("vnp_BankCode", "ACB");
		vnp_Params.put("vnp_TxnRef", vnp_TxnRef);
		vnp_Params.put("vnp_OrderInfo", vnp_OrderInfo);
		vnp_Params.put("vnp_OrderType", "billpayment");

		if (vnp_Locale != null && !vnp_Command.isEmpty()) {
			vnp_Params.put("vnp_Locale", vnp_Locale);
		} else {
			vnp_Params.put("vnp_Locale", "vn");
		}
		vnp_Params.put("vnp_ReturnUrl", vnp_ReturnUrl);
		if(vnp_IpAddr.equals("0:0:0:0:0:0:0:1") || vnp_IpAddr.equals("127.0.0.1")) {
			try {
				InetAddress hostAddress = InetAddress.getLocalHost();
				vnp_IpAddr = hostAddress.getHostAddress();
			} catch (UnknownHostException e) {
				throw new IllegalArgumentException(e);
			}
		}
		vnp_Params.put("vnp_IpAddr", vnp_IpAddr);
		Calendar cld = Calendar.getInstance(TimeZone.getTimeZone("Etc/GMT+7"));

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
		String vnp_CreateDate = formatter.format(cld.getTime());

		vnp_Params.put("vnp_CreateDate", vnp_CreateDate);
		cld.add(Calendar.MINUTE, 15);
		String vnp_ExpireDate = formatter.format(cld.getTime());
		vnp_Params.put("vnp_ExpireDate", vnp_ExpireDate);
		List<String> fieldNames = new ArrayList<>(vnp_Params.keySet());
		Collections.sort(fieldNames);
		StringBuilder hashData = new StringBuilder();
		StringBuilder query = new StringBuilder();
		Iterator<String> itr = fieldNames.iterator();
		while (itr.hasNext()) {
			String fieldName = itr.next();
			String fieldValue = vnp_Params.get(fieldName);
			if ((fieldValue != null) && (fieldValue.length() > 0)) {
				//Build hash data
				hashData.append(fieldName);
				hashData.append('=');
				hashData.append(URLEncoder.encode(fieldValue, StandardCharsets.US_ASCII));
				//Build query
				query.append(URLEncoder.encode(fieldName, StandardCharsets.US_ASCII));
				query.append('=');
				query.append(URLEncoder.encode(fieldValue, StandardCharsets.US_ASCII));

				if (itr.hasNext()) {
					query.append('&');
					hashData.append('&');
				}
			}
		}
		String queryUrl = query.toString();
		String vnp_SecureHash = HmacSecureHash(vnp_SecretHash, hashData.toString(), HmacAlgorithms.HMAC_SHA_512);
		queryUrl += "&vnp_SecureHash=" + vnp_SecureHash;
		return vnp_PaymentViewUrl + "?" + queryUrl;
	}

	public static String HmacSecureHash(String key, String data, HmacAlgorithms algorithm) {
		return new HmacUtils(algorithm, key).hmacHex(data);
	}

	public static String hashAllFields(Map<String, String> fields, String secretKey) {
		List<String> fieldNames = new ArrayList<>(fields.keySet());
		Collections.sort(fieldNames);
		StringBuilder sb = new StringBuilder();
		Iterator<String> itr = fieldNames.iterator();
		while (itr.hasNext()) {
			String fieldName = itr.next();
			String fieldValue = fields.get(fieldName);
			if ((fieldValue != null) && (fieldValue.length() > 0)) {
				sb.append(fieldName);
				sb.append("=");
				sb.append(fieldValue);
			}
			if (itr.hasNext()) {
				sb.append("&");
			}
		}
		return HmacSecureHash(secretKey,sb.toString(), HmacAlgorithms.HMAC_SHA_512);
	}

}