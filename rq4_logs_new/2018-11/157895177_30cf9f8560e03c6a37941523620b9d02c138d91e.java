package fr.utbm.ia54;

import fr.utbm.ia54.Vector2d;
import io.sarl.lang.annotation.SarlElementType;
import io.sarl.lang.annotation.SarlSpecification;
import io.sarl.lang.annotation.SyntheticMember;
import org.eclipse.xtend.lib.annotations.AccessorType;
import org.eclipse.xtend.lib.annotations.Accessors;
import org.eclipse.xtext.xbase.lib.Pure;

/**
 * @author Alexandre Lombard
 */
@SarlSpecification("0.8")
@SarlElementType(10)
@SuppressWarnings("all")
public class BoidBody {
  /**
   * Perception radius of the body
   */
  public final double perceptionRadius = 500.0;
  
  @Accessors(AccessorType.PUBLIC_GETTER)
  private Vector2d position = new Vector2d();
  
  @Accessors(AccessorType.PUBLIC_GETTER)
  private Vector2d velocity = new Vector2d();
  
  public BoidBody() {
  }
  
  public BoidBody(final Vector2d position) {
    Vector2d _vector2d = new Vector2d(position);
    this.position = _vector2d;
  }
  
  public void move(final Vector2d acceleration, final double deltaTime) {
    Vector2d _multiply = acceleration.operator_multiply(Double.valueOf(deltaTime));
    Vector2d _plus = this.velocity.operator_plus(_multiply);
    this.velocity = _plus;
  }
  
  @Override
  @Pure
  @SyntheticMember
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BoidBody other = (BoidBody) obj;
    if (Double.doubleToLongBits(other.perceptionRadius) != Double.doubleToLongBits(this.perceptionRadius))
      return false;
    return super.equals(obj);
  }
  
  @Override
  @Pure
  @SyntheticMember
  public int hashCode() {
    int result = super.hashCode();
    final int prime = 31;
    result = prime * result + (int) (Double.doubleToLongBits(this.perceptionRadius) ^ (Double.doubleToLongBits(this.perceptionRadius) >>> 32));
    return result;
  }
  
  @Pure
  public Vector2d getPosition() {
    return this.position;
  }
  
  @Pure
  public Vector2d getVelocity() {
    return this.velocity;
  }
}