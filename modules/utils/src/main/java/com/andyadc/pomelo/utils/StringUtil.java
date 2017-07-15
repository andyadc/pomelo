package com.andyadc.pomelo.utils;

/**
 * A string utility class that manipulates string.
 *
 * @author andaicheng
 * @version 1.0, 2016-10-07
 */
public final class StringUtil {

    /**
     * A String for a space character.
     */
    public static final String SPACE = " ";
    /**
     * The empty String {@code ""}.
     */
    public static final String EMPTY = "";
    /**
     * A String for linefeed LF ("\n").
     */
    public static final String LF = "\n";
    /**
     * A String for carriage return CR ("\r").
     */
    public static final String CR = "\r";

    /**
     * Cannot instantiate.
     */
    private StringUtil() {
    }

    /**
     * <p>Checks if a CharSequence is empty ("") or null.</p>
     * <p>
     * <pre>
     * StringUtils.isEmpty(null)      = true
     * StringUtils.isEmpty("")        = true
     * StringUtils.isEmpty(" ")       = false
     * StringUtils.isEmpty("bob")     = false
     * StringUtils.isEmpty("  bob  ") = false
     * </pre>
     *
     * @param str the CharSequence to check, may be null
     * @return <code>true</code> if the String is empty or null
     */
    public static boolean isEmpty(final CharSequence str) {
        return str == null || str.length() == 0;
    }

    /**
     * <p>Checks if a CharSequence is not empty ("") and not null.</p>
     * <p>
     * <pre>
     * StringUtils.isNotEmpty(null)      = false
     * StringUtils.isNotEmpty("")        = false
     * StringUtils.isNotEmpty(" ")       = true
     * StringUtils.isNotEmpty("bob")     = true
     * StringUtils.isNotEmpty("  bob  ") = true
     * </pre>
     *
     * @param cs the CharSequence to check, may be null
     * @return {@code true} if the CharSequence is not empty and not null
     */
    public static boolean isNotEmpty(final CharSequence cs) {
        return !isEmpty(cs);
    }

    /**
     * /**
     * <p>Returns either the passed in CharSequence, or if the CharSequence is
     * empty or {@code null}, the value of {@code defaultStr}.</p>
     * <p>
     * <pre>
     * StringUtils.defaultIfEmpty(null, "NULL")  = "NULL"
     * StringUtils.defaultIfEmpty("", "NULL")    = "NULL"
     * StringUtils.defaultIfEmpty(" ", "NULL")   = " "
     * StringUtils.defaultIfEmpty("bat", "NULL") = "bat"
     * StringUtils.defaultIfEmpty("", null)      = null
     * </pre>
     *
     * @param <T>        the specific kind of CharSequence
     * @param str        the CharSequence to check, may be null
     * @param defaultStr the default CharSequence to return
     *                   if the input is empty ("") or {@code null}, may be null
     * @return the passed in CharSequence, or the default
     */
    public static <T extends CharSequence> T defaultIfEmpty(final T str, final T defaultStr) {
        return isEmpty(str) ? defaultStr : str;
    }

    /**
     * <p>Checks if a CharSequence is whitespace, empty ("") or null.</p>
     * <p>
     * <pre>
     * StringUtils.isBlank(null)      = true
     * StringUtils.isBlank("")        = true
     * StringUtils.isBlank(" ")       = true
     * StringUtils.isBlank("bob")     = false
     * StringUtils.isBlank("  bob  ") = false
     * </pre>
     *
     * @param str the CharSequence to check, may be null
     * @return <code>true</code> if the String is null, empty or whitespace
     */
    public static boolean isBlank(final CharSequence str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * * <p>Checks if a CharSequence is not empty (""), not null and not whitespace only.</p>
     * <p>
     * <pre>
     * StringUtils.isNotBlank(null)      = false
     * StringUtils.isNotBlank("")        = false
     * StringUtils.isNotBlank(" ")       = false
     * StringUtils.isNotBlank("bob")     = true
     * StringUtils.isNotBlank("  bob  ") = true
     * </pre>
     *
     * @param cs the CharSequence to check, may be null
     * @return {@code true} if the CharSequence is
     * not empty and not null and not whitespace
     */
    public static boolean isNotBlank(final CharSequence cs) {
        return !isBlank(cs);
    }

    /**
     * * <p>Returns either the passed in CharSequence, or if the CharSequence is
     * whitespace, empty ("") or {@code null}, the value of {@code defaultStr}.</p>
     * <p>
     * <pre>
     * StringUtils.defaultIfBlank(null, "NULL")  = "NULL"
     * StringUtils.defaultIfBlank("", "NULL")    = "NULL"
     * StringUtils.defaultIfBlank(" ", "NULL")   = "NULL"
     * StringUtils.defaultIfBlank("bat", "NULL") = "bat"
     * StringUtils.defaultIfBlank("", null)      = null
     * </pre>
     *
     * @param <T>        the specific kind of CharSequence
     * @param str        the CharSequence to check, may be null
     * @param defaultStr the default CharSequence to return
     *                   if the input is whitespace, empty ("") or {@code null}, may be null
     * @return the passed in CharSequence, or the default
     */
    public static <T extends CharSequence> T defaultIfBlank(final T str, final T defaultStr) {
        return isBlank(str) ? defaultStr : str;
    }

    /**
     * Checks if some CharSequences has any blank string.<br>
     * If strings == null, return true
     *
     * @param strings the CharSequences
     * @return true if any CharSequence is blank, otherwise false.
     */
    public static boolean isAnyBlank(final CharSequence... strings) {
        if (strings == null) {
            return true;
        }
        for (CharSequence s : strings) {
            if (isBlank(s)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if all the CharSequences is not blank.
     *
     * @param strings the CharSequences
     * @return true if all CharSequence is not blank, otherwise false.
     */
    public static boolean isAllNotBlank(final CharSequence... strings) {
        return !isAnyBlank(strings);
    }

    /**
     * <p>
     * Checks if some CharSequences has any empty string.
     * </p>
     * If strings == null, return true
     *
     * @param strings the CharSequences
     * @return true is any CharSequence is empty, otherwise false.
     */
    public static boolean isAnyEmpty(final CharSequence... strings) {
        if (strings == null) {
            return true;
        }
        for (CharSequence s : strings) {
            if (isEmpty(s)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if all the CharSequences is not empty.
     *
     * @param strings the CharSequences
     * @return true if all CharSequence is not empty, otherwise false.
     */
    public static boolean isAllNotEmpty(final CharSequence... strings) {
        return !isAnyEmpty(strings);
    }

    /**
     * <p>Reverses a String as per {@link StringBuilder#reverse()}.</p>
     * <p>A {@code null} String returns {@code null}.</p>
     * <p>
     * <pre>
     * StringUtils.reverse(null)  = null
     * StringUtils.reverse("")    = ""
     * StringUtils.reverse("bat") = "tab"
     * </pre>
     *
     * @param str the String to reverse, may be null
     * @return the reversed String, {@code null} if null String input
     */
    public static String reverse(final String str) {
        if (str == null) {
            return null;
        }
        return new StringBuilder(str).reverse().toString();
    }

    /**
     * Returns a string consisting of a specific number of concatenated copies of an input string. For
     * example, {@code repeat("hey", 3)} returns the string {@code "heyheyhey"}.
     *
     * @param string any non-null string
     * @param count  the number of times to repeat it; a nonnegative integer
     * @return a string containing {@code string} repeated {@code count} times (the empty string if
     * {@code count} is zero)
     * @throws IllegalArgumentException if {@code count} is negative
     */
    public static String repeat(final String string, final int count) {
        if (isEmpty(string)) {
            return "";
        }

        if (count <= 1) {
            Assert.isTrue(count >= 0, "invalid count: " + count);
            return (count == 0) ? "" : string;
        }

        // IF YOU MODIFY THE CODE HERE, you must update StringsRepeatBenchmark
        final int len = string.length();
        final long longSize = (long) len * (long) count;
        final int size = (int) longSize;
        if (size != longSize) {
            throw new ArrayIndexOutOfBoundsException("Required array size too large: " + longSize);
        }

        final char[] array = new char[size];
        string.getChars(0, len, array, 0);
        int n;
        for (n = len; n < size - n; n <<= 1) {
            System.arraycopy(array, 0, array, n, n);
        }
        System.arraycopy(array, 0, array, n, size - n);
        return new String(array);
    }
}
