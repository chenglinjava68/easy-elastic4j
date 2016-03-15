package com.github.wens.elastic;

import java.math.BigDecimal;

/**
 * Created by wens on 15-10-22.
 */
public class Strings {

    public static boolean isFilled(Object value) {
        if (value == null) {
            return false;
        }

        if (value instanceof String) {
            return ((String) value).trim().length() != 0;
        }
        return false;
    }

    public static boolean isEmpty(String value) {
        if (value == null || value.trim().length() == 0) return true;
        return false;
    }

    public static String toString(Object value) {

        if (value instanceof String) {
            return (String) value;
        }

        if (value instanceof Integer) {
            return String.valueOf(value);
        }
        if (value instanceof Long) {
            return String.valueOf(value);
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }
        if (value instanceof Double) {
            return String.valueOf(value);
        }
        if (value.getClass().isEnum()) {
            return ((Enum<?>) value).name();
        }
        if (value instanceof Float) {
            return String.valueOf(value);
        }

        return value.toString();

    }
}
