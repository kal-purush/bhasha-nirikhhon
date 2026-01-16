package com.sarality.action.intent;

import android.view.View;

/**
 * A generic ViewValueExtractor that returns a constant value that was passed in the
 * constructor
 *
 * @author abhideep@ (Abhideep Singh)
 */
public class ConstantViewValueExtractor<T> implements ViewValueExtractor<T> {
  private final T value;

  public ConstantViewValueExtractor(T value) {
    this.value = value;
  }

  @Override
  public T extract(View view) {
    return value;
  }
}