package com.andyadc.pomelo.utils;

/**
 * Assertion utility class that assists in validating arguments.
 * <p>
 * <p>Useful for identifying programmer errors early and clearly at runtime.
 *
 * @author andaicheng
 * @version 1.0.0
 */
public final class Assert {

    /**
     * Cannot instantiate.
     */
    private Assert() {
    }

    /**
     * Assert a boolean expression, throwing {@code IllegalArgumentException}
     * if the test result is {@code false}.
     *
     * @param expression a boolean expression
     * @param message    the exception message to use if the assertion fails
     * @throws IllegalArgumentException if expression is {@code false}
     */
    public static void isTrue(final boolean expression, final String message) {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Assert a boolean expression, throwing {@code IllegalArgumentException}
     * if the test result is {@code false}.
     *
     * @param expression a boolean expression
     * @throws IllegalArgumentException if expression is {@code false}
     */
    public static void isTrue(final boolean expression) {
        isTrue(expression, "[Assertion failed] - this expression must be true");
    }

    /**
     * Assert that an object is {@code null} .
     * <pre class="code">Assert.isNull(value, "The value must be null");</pre>
     *
     * @param object  the object to check
     * @param message the exception message to use if the assertion fails
     * @throws IllegalArgumentException if the object is not {@code null}
     */
    public static void isNull(final Object object, final String message) {
        if (object != null) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Assert that an object is {@code null}.
     * <pre class="code">Assert.isNull(value);</pre>
     *
     * @param object the object to check
     * @throws IllegalArgumentException if the object is not {@code null}
     */
    public static void isNull(final Object object) {
        isNull(object, "[Assertion failed] - the object argument must be null");
    }

    /**
     * Assert that an object is not {@code null}.
     * <pre class="code">Assert.notNull(clazz, "The class must not be null");</pre>
     *
     * @param object  the object to check
     * @param message the exception message to use if the assertion fails
     * @throws IllegalArgumentException if the object is {@code null}
     */
    public static void notNull(final Object object, final String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Assert that an object is not {@code null} .
     * <pre class="code">Assert.isNull(value);</pre>
     *
     * @param object the object to check
     * @throws IllegalArgumentException if the object is {@code null}
     */
    public static void notNull(final Object object) {
        notNull(object, "[Assertion failed] - this argument is required; it must not be null");
    }

    /**
     * Assert that the given String has valid text content; that is, it must not
     * be {@code null} and must contain at least one non-whitespace character.
     * <pre class="code">Assert.hasText(name, "'name' must not be empty");</pre>
     *
     * @param text    the String to check
     * @param message the exception message to use if the assertion fails
     * @throws IllegalArgumentException if the text does not contain valid text content
     */
    public static void hasText(final String text, final String message) {
        if (StringUtil.isBlank(text)) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Assert that the given String has valid text content; that is, it must not
     * be {@code null} and must contain at least one non-whitespace character.
     *
     * @param text the String to check
     * @throws IllegalArgumentException if the text does not contain valid text content
     */
    public static void hasText(final String text) {
        hasText(text,
                "[Assertion failed] - this String argument must have text; it must not be null, empty, or blank");
    }
}
