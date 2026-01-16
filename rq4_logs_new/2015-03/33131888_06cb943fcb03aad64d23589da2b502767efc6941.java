/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * Copyright 2012-2015 the original author or authors.
 */
package org.assertj.swing.core.matcher;

import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.swing.text.JTextComponent;
import org.assertj.swing.annotation.RunsInCurrentThread;
import static org.assertj.swing.core.matcher.NamedComponentMatcherTemplate.anyValue;

/**
 *
 * @author Iñaki Ibarrola
 */
public final class JLabeledTextComponentMatcher extends LabeledComponentMatcherTemplate<JTextComponent> {

    private Object text;

    /**
     * <p>
     * Creates a new {@link JLabeledTextComponentMatcher} that matches a
     * {@code JTextComponent} by name.
     * </p>
     *
     * <p>
     * The following code listing shows how to match a {@code JTextComponent} by
     * name and text:
     *
     * <pre>
     * JLabeledTextComponentMatcher m = {@link #withName(String) withName}("lastName").{@link #andText(String) andText}("Wang");
     * </pre>
     *
     * </p>
     *
     * <p>
     * The following code listing shows how to match a {@code JTextComponent},
     * that should be showing on the screen, by name and text:
     *
     * <pre>
     * JLabeledTextComponentMatcher m = {@link #withName(String) withName}("lastName").{@link #andText(String) andText}("Wang").{@link #andShowing() andShowing}();
     * </pre>
     *
     * </p>
     *
     * @param name the id to match.
     * @return the created matcher.
     */
    public static @Nonnull
    JLabeledTextComponentMatcher withName(@Nullable String name) {
        return new JLabeledTextComponentMatcher(name, anyValue());
    }

    /**
     * <p>
     * Creates a new {@link JLabeledTextComponentMatcher} that matches a
     * {@code JTextComponent} by text.
     * </p>
     *
     * <p>
     * The following code listing shows how to match a {@code JTextComponent} by
     * text:
     *
     * <pre>
     * JLabeledTextComponentMatcher m = {@link #withText(String) withText}("Wang");
     * </pre>
     *
     * </p>
     *
     * <p>
     * The following code listing shows how to match a {@code JTextComponent},
     * that should be showing on the screen, by text:
     *
     * <pre>
     * JLabeledTextComponentMatcher m = {@link #withText(String) withText}("Wang").{@link #andShowing() andShowing}();
     * </pre>
     *
     * </p>
     *
     * @param text the text to match. It can be a regular expression.
     * @return the created matcher.
     */
    public static @Nonnull
    JLabeledTextComponentMatcher withText(@Nonnull String text) {
        return new JLabeledTextComponentMatcher(anyValue(), text);
    }

    /**
     * <p>
     * Creates a new {@link JLabeledTextComponentMatcher} that matches a
     * {@code JTextComponent} by its text.
     * </p>
     *
     * <p>
     * The following code listing shows how to match a {@code JTextComponent} by
     * text, using a regular expression pattern:
     *
     * <pre>
     * JLabeledTextComponentMatcher m = {@link #withText(Pattern) withText}(Pattern.compile("W.*"));
     * </pre>
     *
     * </p>
     *
     * <p>
     * The following code listing shows how to match a {@code JTextComponent},
     * that should be showing on the screen, by text, using a regular expression
     * pattern:
     *
     * <pre>
     * JLabeledTextComponentMatcher m = {@link #withText(Pattern) withText}(Pattern.compile("W.*")).{@link #andShowing() andShowing}();
     * </pre>
     *
     * </p>
     *
     * @param textPattern the text to match. It can be a regular expression.
     * @return the created matcher.
     */
    public static @Nonnull
    JLabeledTextComponentMatcher withText(@Nonnull Pattern textPattern) {
        return new JLabeledTextComponentMatcher(anyValue(), textPattern);
    }

    public static @Nonnull
    JLabeledTextComponentMatcher withLabelName(String name) {
        return new JLabeledTextComponentMatcher(anyValue(), anyValue(), name, anyValue());
    }

    public static @Nonnull
    JLabeledTextComponentMatcher withLabelText(String text) {
        return new JLabeledTextComponentMatcher(anyValue(), anyValue(), anyValue(), text);
    }

    public static @Nonnull
    JLabeledTextComponentMatcher withLabelText(Pattern pattern) {
        return new JLabeledTextComponentMatcher(anyValue(), anyValue(), anyValue(), pattern);
    }

    /**
     * Creates a new {@link JLabeledTextComponentMatcher} that matches any
     * {@code JTextComponent}.
     *
     * @return the created matcher.
     */
    public static @Nonnull
    JLabeledTextComponentMatcher any() {
        return new JLabeledTextComponentMatcher(anyValue(), anyValue());
    }

    private JLabeledTextComponentMatcher(@Nullable Object name, @Nullable Object text) {
        super(JTextComponent.class, name);
        this.text = text;
    }

    private JLabeledTextComponentMatcher(@Nullable Object name, @Nullable Object text, Object labelName, Object labelText) {
        super(JTextComponent.class, name, labelName, labelText);
        this.text = text;
    }

    /**
     * Specifies the name of the label to match. If this matcher was created
     * using {@link #withLabelName(String)}, this method will simply update the
     * name to match.
     *
     * @param name the new label name to match.
     * @return this matcher.
     */
    public @Nonnull
    JLabeledTextComponentMatcher andLabelName(@Nonnull String name) {
        super.setLabelName(name);
        return this;
    }

    /**
     * Specifies the text to match. If this matcher was created using
     * {@link #withText(String)} or {@link #withText(Pattern)}, this method will
     * simply update the text to match.
     *
     * @param newText the new text to match. It can be a regular expression.
     * @return this matcher.
     */
    public @Nonnull
    JLabeledTextComponentMatcher andText(@Nonnull String newText) {
        text = newText;
        return this;
    }

    /**
     * Specifies the text of the label to match. If this matcher was created
     * using {@link #withLabelText(String)} or {@link #withLabelText(Pattern)},
     * this method will simply update the text to match.
     *
     * @param newText the new text to match. It can be a regular expression.
     * @return this matcher.
     */
    public @Nonnull
    JLabeledTextComponentMatcher andLabelText(@Nonnull String newText) {
        setLabelText(newText);
        return this;
    }

    /**
     * Specifies the text to match. If this matcher was created using
     * {@link #withText(String)} or {@link #withText(Pattern)}, this method will
     * simply update the text to match.
     *
     * @param textPattern the regular expression pattern to match.
     * @return this matcher.
     */
    public @Nonnull
    JLabeledTextComponentMatcher andText(@Nonnull Pattern textPattern) {
        text = textPattern;
        return this;
    }

    /**
     * Indicates that the {@code JTextComponent} to match should be showing on
     * the screen.
     *
     * @return this matcher.
     */
    public @Nonnull
    JLabeledTextComponentMatcher andShowing() {
        requireShowing(true);
        return this;
    }

    /**
     * <p>
     * Indicates whether the text of the given {@code JTextComponent} is equal
     * to the text in this matcher.
     * </p>
     *
     * <p>
     * <b>Note:</b> This method is accessed in the current executing thread.
     * Such thread may or may not be the event dispatch thread (EDT.) Client
     * code must call this method from the EDT.
     * </p>
     *
     * @param textComponent the {@code JTextComponent} to match.
     * @return {@code true} if the text in the {@code JTextComponent} is equal
     * to the text in this matcher, {@code false} otherwise.
     */
    @RunsInCurrentThread
    @Override
    protected boolean isMatching(@Nonnull JTextComponent textComponent) {
        return isComponentMatching(textComponent);
    }

    @Override
    protected boolean isExtendedMatching(JTextComponent textComponent) {
        return isNameMatching(textComponent.getName()) && arePropertyValuesMatching(text, textComponent.getText());
    }

    @Override
    public String toString() {
        String format = "%s[name=%s, text=%s, labelName=%s, labelText=%s, requireShowing=%b]";
        return String.format(format, getClass().getName(), quotedName(), quoted(text), labelMatcher.quotedName(), quoted(labelText), requireShowing());
    }
}