package com.fasterxml.slavedriver.util;

import java.util.Collection;
import java.util.Map;

public class Strings
{
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
}
