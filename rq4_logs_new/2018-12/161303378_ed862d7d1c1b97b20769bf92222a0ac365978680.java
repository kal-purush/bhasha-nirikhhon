package com.xinyilian.text.utils;

import android.graphics.Color;

import cn.cloudcore.iprotect.plugin.CEditText;
import cn.cloudcore.iprotect.plugin.CEditTextAttrSet;
import cn.cloudcore.iprotect.view.CEditTextView;

public class AttrsUtils {
    
    // 应用平台模数
    static public String modulus_app = "8DC461F00F614662FD6C4401527A8FC51BF89EBBE4A32BC26740D659625A782B3FC2C0B18AEF1069AE748D3D614C8FBBDE49D5CA0F7A63F8565B6DF8ED82D397D4F760E1B3DED6B446AA7FD6D798E9B443712AF0335D7DB591024F1B6BB581F84FF6AF057FE86BB1FAFD1C3B9FF9F9D571A8497D84C78F607D8C6DC9A256BFDB";
    // 加密平台模数，由加密机解密
    static public String modulus_enc = "a0661982beb093b0ccc2af5987a19ba6f344a283c68b3bc27965e2fc1def3cf8a730675c667c99b3691731ca882c75409bc391593128a01fdcd3d0383678060aeefe3afe5347ad334a4d8016ee5e967deb07c2054aa681d725bd4a50b97fa8294eb41e29c88cefc86d911d57572f410570525f8c1c4b910d3fb1c40c88a5d3e3";
    
    // 加密平台国密公钥，由加密机解密
    static public String eccX = "36589faa2cfb61d807678be9d5e15d74078b012e153ca99ba98fc22ad82fd138";
    static public String eccY = "e8fc8722154aded66f00e62aaea303f30833ee36f1f28339c484adef84e03769";
    
    static public CEditTextAttrSet getQueryAttrSet(String name) {
        CEditTextAttrSet attrSet = new CEditTextAttrSet();
        
        // 每个密码框的名字不能相同
        attrSet.name = name;
        
        // 输入框内容颜色
        attrSet.textColor = Color.BLACK;
           
        // 键盘式样，全键盘 or 数字键盘
        attrSet.softkbdType = CEditTextAttrSet.SOFT_KBD_QWERTY;
        // attrSet.softkbdType = CEditTextAttrSet.SOFT_KBD_NUMBER;
        
        // 键盘上方是否有输入框
        attrSet.softkbdView = CEditTextAttrSet.SOFT_KBD_NO_EDITTEXT;
        // attrSet.softkbdView = CEditTextAttrSet.SOFT_KBD_WITH_EDITTEXT;
        
        // 全键盘初始显示的键盘： 小写、大写、第一页符号、第二页符号。
        attrSet.softkbdStype = CEditTextAttrSet.SOFT_KBD_QWERTY_ALPHA_LOWER;
        // attrSet.softkbdStype = CEditTextAttrSet.SOFT_KBD_QWERTY_ALPHA_UPPER;
        // attrSet.softkbdStype = CEditTextAttrSet.SOFT_KBD_QWERTY_SYMBOL_1;
        // attrSet.softkbdStype = CEditTextAttrSet.SOFT_KBD_QWERTY_SYMBOL_2;
        
        // 点击按键是否有变化
        attrSet.softkbdMode = CEditTextAttrSet.SOFT_KBD_MODE_KDOWN_CHANGE;
        // attrSet.softkbdMode = CEditTextAttrSet.SOFT_KBD_MODE_KDOWN_NOCHANGE;
        
        // 点击按键有反应时，全键盘是否有冒泡效果
        // attrSet.softkbdMode = CEditTextAttrSet.SOFT_KBD_MODE_KDOWN_NOPOP;
        
        // 全键盘切换模式： 字母与符号切换，字母与数字切换，字母、符号、数字切换。
        attrSet.switchMode  = CEditTextAttrSet.SOFT_KBD_SWITCH_BETWEEN_ALPHA_SYMBOL;
        // attrSet.switchMode  = CEditTextAttrSet.SOFT_KBD_SWITCH_BETWEEN_ALPHA_NUMBER;
        // attrSet.switchMode  = CEditTextAttrSet.SOFT_KBD_SWITCH_ALPHA_SYMBOL_NUMBER;

        attrSet.kbdRandom = true;       // 键盘按钮是否乱序

        /* 两种乱序模式： 
               全键盘按行乱序，数字键盘随机乱序
               只数字键盘乱序
         */
        attrSet.kbdRandomType = CEditTextAttrSet.SOFT_KBD_RANDOM_TYPE_ALL;
        // attrSet.kbdRandomType = CEditTextAttrSet.SOFT_KBD_RANDOM_TYPE_NUMBER;
        
        attrSet.clearWhenOpenKbd = false;   // 打开时是否清楚以前输入的内容
        attrSet.kbdVibrator = true;         // 是否震动
        attrSet.contentType = 0;            // 见文档
        
        attrSet.maxLength = 12;             
        attrSet.maxLength = 6;
        
        attrSet.maskChar = '*';             // 输入框显示的掩码内容 
        attrSet.accepts = "[!-~]+";         // 允许输入的内容
        
        return attrSet;
    }
    
    static public CEditTextAttrSet getNumberAttrSet(String name) {
        CEditTextAttrSet attrSet = new CEditTextAttrSet();
        
        // 每个密码框的名字不能相同
        attrSet.name = name;
        
        // 输入框内容颜色
        attrSet.textColor = Color.BLACK;
           
        // 键盘式样，全键盘 or 数字键盘
        // attrSet.softkbdType = CEditTextAttrSet.SOFT_KBD_QWERTY;
        attrSet.softkbdType = CEditTextAttrSet.SOFT_KBD_NUMBER;
        
        // 键盘上方是否有输入框
        attrSet.softkbdView = CEditTextAttrSet.SOFT_KBD_NO_EDITTEXT;
        // attrSet.softkbdView = CEditTextAttrSet.SOFT_KBD_WITH_EDITTEXT;
        
        // 全键盘初始显示的键盘： 小写、大写、第一页符号、第二页符号。
        attrSet.softkbdStype = CEditTextAttrSet.SOFT_KBD_QWERTY_ALPHA_LOWER;
        // attrSet.softkbdStype = CEditTextAttrSet.SOFT_KBD_QWERTY_ALPHA_UPPER;
        // attrSet.softkbdStype = CEditTextAttrSet.SOFT_KBD_QWERTY_SYMBOL_1;
        // attrSet.softkbdStype = CEditTextAttrSet.SOFT_KBD_QWERTY_SYMBOL_2;
        
        // 点击按键是否有变化
        attrSet.softkbdMode = CEditTextAttrSet.SOFT_KBD_MODE_KDOWN_CHANGE;
        // attrSet.softkbdMode = CEditTextAttrSet.SOFT_KBD_MODE_KDOWN_NOCHANGE;
        
        // 点击按键有反应时，全键盘是否有冒泡效果
        // attrSet.softkbdMode = CEditTextAttrSet.SOFT_KBD_MODE_KDOWN_NOPOP;
        
        // 全键盘切换模式： 字母与符号切换，字母与数字切换，字母、符号、数字切换。
        attrSet.switchMode  = CEditTextAttrSet.SOFT_KBD_SWITCH_BETWEEN_ALPHA_SYMBOL;
        // attrSet.switchMode  = CEditTextAttrSet.SOFT_KBD_SWITCH_BETWEEN_ALPHA_NUMBER;
        // attrSet.switchMode  = CEditTextAttrSet.SOFT_KBD_SWITCH_ALPHA_SYMBOL_NUMBER;

        attrSet.kbdRandom = true;       // 键盘按钮是否乱序

        /* 两种乱序模式： 
               全键盘按行乱序，数字键盘随机乱序
               只数字键盘乱序
         */
        attrSet.kbdRandomType = CEditTextAttrSet.SOFT_KBD_RANDOM_TYPE_ALL;
        // attrSet.kbdRandomType = CEditTextAttrSet.SOFT_KBD_RANDOM_TYPE_NUMBER;
        
        attrSet.clearWhenOpenKbd = false;   // 打开时是否清楚以前输入的内容
        attrSet.kbdVibrator = true;         // 是否震动
        attrSet.contentType = 0;            // 见文档
        
        attrSet.maxLength = 12;             
        attrSet.maxLength = 6;
        
        attrSet.maskChar = '*';             // 输入框显示的掩码内容 
        attrSet.accepts = "[!-~]+";         // 允许输入的内容
        
        return attrSet;
    }
    
    static public void setOneLayerCipher(CEditText cEditText) {
        cEditText.publicKeyAppModulus(modulus_app);   // 一次加密  -- 云核标准数字信封
    }
    
    static public void setTwoLayerRSACipher(CEditText cEditText) {
        cEditText.publicKeyModulus(modulus_enc);      // 第一层加密  -- RSA算法，根据加密机算法确定
        cEditText.publicKeyAppModulus(modulus_app);   // 第二层加密  -- 云核标准数字信封
        cEditText.setAlgorithmCode("001");            // 根据加密机算法，切换第一层加密算法
    }
    
    static public void setTwoLayerSM2Cipher(CEditText cEditText) {
        cEditText.publicKeyECC(eccX, eccY);           // 第一层层加密  -- 国密算法
        // 国密密文格式：密文格式为：长度(4字节)||C1X(32字节)||C1Y(32字节)||C2(4字节对齐)||C3(32字节)
        
        cEditText.publicKeyAppModulus(modulus_app);   // 第二层加密  -- 云核标准数字信封        
        cEditText.setAlgorithmCode("004");            // 根据加密机算法，切换第一层加密算法，国密算法通常为004
    }
    
    static public void setOneLayerCipher(CEditTextView cEditText) {
        cEditText.publicKeyAppModulus(modulus_app);   // 一次加密  -- 云核标准数字信封
    }
    
    static public void setTwoLayerRSACipher(CEditTextView cEditText) {
        cEditText.publicKeyModulus(modulus_enc);      // 第一层加密  -- RSA算法，根据加密机算法确定
        cEditText.publicKeyAppModulus(modulus_app);   // 第二层加密  -- 云核标准数字信封
        cEditText.setAlgorithmCode("001");            // 根据加密机算法，切换第一层加密算法
    }
    
    static public void setTwoLayerSM2Cipher(CEditTextView cEditText) {
        cEditText.publicKeyECC(eccX, eccY);           // 第一层层加密  -- 国密算法
        // 国密密文格式：密文格式为：长度(4字节)||C1X(32字节)||C1Y(32字节)||C2(4字节对齐)||C3(32字节)
        
        cEditText.publicKeyAppModulus(modulus_app);   // 第二层加密  -- 云核标准数字信封        
        cEditText.setAlgorithmCode("004");            // 根据加密机算法，切换第一层加密算法，国密算法通常为004
    }

}