// Generated code from Butter Knife. Do not modify!
package maciej.example.com.storeapplication;

import android.support.annotation.CallSuper;
import android.support.annotation.UiThread;
import android.view.View;
import android.widget.ImageView;
import android.widget.TextView;
import butterknife.Unbinder;
import butterknife.internal.Utils;
import maciej.example.com.storeapplication.activity.ProductItemActivity;

import java.lang.IllegalStateException;
import java.lang.Override;

public class ProductItemActivity_ViewBinding<T extends ProductItemActivity> implements Unbinder {
  protected T target;

  @UiThread
  public ProductItemActivity_ViewBinding(T target, View source) {
    this.target = target;

    target.addProduct = Utils.findRequiredViewAsType(source, R.id.cart_plus_img, "field 'addProduct'", ImageView.class);
    target.subtractProduct = Utils.findRequiredViewAsType(source, R.id.cart_minus_img, "field 'subtractProduct'", ImageView.class);
    target.productQuantity = Utils.findRequiredViewAsType(source, R.id.cart_product_quantity, "field 'productQuantity'", TextView.class);
  }

  @Override
  @CallSuper
  public void unbind() {
    T target = this.target;
    if (target == null) throw new IllegalStateException("Bindings already cleared.");

    target.addProduct = null;
    target.subtractProduct = null;
    target.productQuantity = null;

    this.target = null;
  }
}