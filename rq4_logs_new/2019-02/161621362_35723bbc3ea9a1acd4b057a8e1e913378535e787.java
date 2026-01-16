package com.payline.payment.oney.utils;

public class OneyCheckConstants {

    private OneyCheckConstants() {
    }

    public static final String TEST_JSON_MSG = "{\"language_code\":\"#LANGUAGE_CODE#\",\"skin_id\":\"0\",\"merchant_language_code\":\"#LANGUAGE_CODE#\",\"merchant_request_id\":\"xxx\",\"purchase\":{\"external_reference_type\":\"CMDE\",\"external_reference\":\"0\",\"purchase_amount\":\"0\",\"currency_code\":\"EUR\",\"purchase_merchant\":{\"merchant_guid\":\"#MERCHANT_GUID_KEY#\",\"external_reference\":\"0\"},\"delivery\":{\"delivery_date\":\"2019-01-17\",\"delivery_mode_code\":\"1\",\"delivery_option\":\"2\",\"address_type\":\"5\",\"recipient\":{\"recipient_honorific_code\":\"1\",\"surname\":\"zippJaRitN\",\"first_name\":\"YiThaqv\",\"phone_number\":\"+328212106983\"},\"delivery_address\":{\"line1\":\"898 rue ZKLKB\",\"line4\":\"residence vGZTcmuYE\",\"postal_code\":\"1000\",\"municipality\":\"Bruxelles\",\"country_code\":\"#COUNTRY_ADDRESS#\",\"country_label\":\"Belgique\"}},\"item_list\":[{\"is_main_item\":\"1\",\"category_code\":\"15\",\"label\":\"some label\",\"item_external_code\":\"item1\",\"quantity\":\"4\",\"price\":\"0.0\"},{\"is_main_item\":\"0\",\"category_code\":\"15\",\"label\":\"some label\",\"item_external_code\":\"item2\",\"quantity\":\"4\",\"price\":\"0.0\"}],\"number_of_items\":\"2\"},\"customer\":{\"customer_external_code\":\"subscriber12\",\"language_code\":\"#LANGUAGE_CODE#\",\"identity\":{\"person_type\":\"2\",\"honorific_code\":\"1\",\"birth_name\":\"KKjFXbghin\",\"first_name\":\"wzMXFJO\",\"birth_date\":\"1981-05-04\"},\"contact_details\":{\"landline_number\":\"+322760879161\",\"mobile_phone_number\":\"+322760879161\",\"email_address\":\"testoney.jKjfL@gmail.com\"},\"customer_address\":{\"line1\":\"898 rue ZKLKB\",\"line4\":\"residence vGZTcmuYE\",\"postal_code\":\"1000\",\"municipality\":\"Bruxelles\",\"country_code\":\"#COUNTRY_ADDRESS#\",\"country_label\":\"Belgique\"}},\"payment\":{\"payment_amount\":\"0\",\"currency_code\":\"EUR\",\"payment_type\":\"0\",\"business_transaction\":{\"code\":\"#OPC_KEY#\"}},\"navigation\":{\"server_response_url\":\"http://google.com/\",\"success_url\":\"https://succesurl.com/\",\"fail_url\":\"http://localhost/cancelurl.com/\",\"alternative_return_url\":\"https://succesurl.com/\"},\"merchant_context\":\"softDescriptor\",\"psp_context\":\"1412171547724188240\",\"merchant_guid\":\"#MERCHANT_GUID_KEY#\",\"psp_guid\":\"#PSP_GUID_KEY#\"}";


    public static final String OPC_KEY_TAG = "#OPC_KEY#";
    public static final String MERCHANT_GUID_TAG = "#MERCHANT_GUID_KEY#";
    public static final String PSP_GUID_TAG = "#PSP_GUID_KEY#";
    public static final String COUNTRY_ADDRESS = "#COUNTRY_ADDRESS#";
    public static final String LANGUAGE_CODE = "#LANGUAGE_CODE#";


}