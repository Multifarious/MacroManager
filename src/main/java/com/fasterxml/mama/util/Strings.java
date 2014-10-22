package com.fasterxml.mama.util;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;

/**
 * Silly utility class to help porting String manipulation over.
 */
public class Strings
{
    private final static byte[] NO_BYTES = new byte[0];
    private final static Charset UTF8 = Charset.forName("UTF-8");
    
    public static byte[] utf8BytesFrom(String value) {
        return (value == null) ? NO_BYTES : value.getBytes(UTF8);
    }

    public static String stringFromUtf8(byte[] data) {
        if (data == null || data.length == 0) {
            return "";
        }
        return new String(data, UTF8);
    }

    public static String mkstring(Collection<?> c, String sep)
    {
        StringBuilder sb = new StringBuilder(50);
        for (Object ob : c) {
            if (sb.length() > 0) {
                sb.append(sep);
            }
            sb.append(String.valueOf(ob));
        }
        return sb.toString();
    }

    public static String mkstring(Map<String,?> map, String sep)
    {
        StringBuilder sb = new StringBuilder(50);
        for (Map.Entry<String,?> entry : map.entrySet()) {
            if (sb.length() > 0) {
                sb.append(sep);
            }
            sb.append(entry.getKey());
            sb.append('=');
            sb.append(String.valueOf(entry.getValue()));
        }
        return sb.toString();
    }

    public static String mkstringForKeys(Map<String,?> map, String sep) {
        return mkstring(map.keySet(), sep);
    }

    public static String mkstringForValues(Map<String,?> map, String sep) {
        return mkstring(map.values(), sep);
    }
}
