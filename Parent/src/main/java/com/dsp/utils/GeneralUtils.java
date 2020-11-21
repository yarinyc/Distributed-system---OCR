package com.dsp.utils;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.Random;

public class GeneralUtils {

    public static String toBase64(String data) {
        String base64UserData = null;
        try {
            base64UserData = new String(Base64.getEncoder().encode(data.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return base64UserData;
    }

    public static String getUniqueID() {
        Long l = new Random().nextLong();
        return Long.toHexString(l);
    }
}
