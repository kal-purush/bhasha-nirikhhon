package hu.bme.mit.inf.dipterv1.incquery.patterns;

import hu.bme.mit.inf.dipterv1.incquery.patterns.util.Containment_socialNetwork_familyQuerySpecification;
import java.util.Arrays;
import java.util.List;
import org.eclipse.incquery.runtime.api.IPatternMatch;
import org.eclipse.incquery.runtime.api.impl.BasePatternMatch;
import org.eclipse.incquery.runtime.exception.IncQueryException;
import socialnetwork_base.Man;
import socialnetwork_base.SocialNetwork;
import socialnetwork_base.Woman;

/**
 * Pattern-specific match representation of the hu.bme.mit.inf.dipterv1.incquery.patterns.containment_socialNetwork_family pattern,
 * to be used in conjunction with {@link Containment_socialNetwork_familyMatcher}.
 * 
 * <p>Class fields correspond to parameters of the pattern. Fields with value null are considered unassigned.
 * Each instance is a (possibly partial) substitution of pattern parameters,
 * usable to represent a match of the pattern in the result of a query,
 * or to specify the bound (fixed) input parameters when issuing a query.
 * 
 * @see Containment_socialNetwork_familyMatcher
 * @see Containment_socialNetwork_familyProcessor
 * 
 */
@SuppressWarnings("all")
public abstract class Containment_socialNetwork_familyMatch extends BasePatternMatch {
  private SocialNetwork fSn;
  
  private Man fF;
  
  private Woman fM;
  
  private static List<String> parameterNames = makeImmutableList("sn", "f", "m");
  
  private Containment_socialNetwork_familyMatch(final SocialNetwork pSn, final Man pF, final Woman pM) {
    this.fSn = pSn;
    this.fF = pF;
    this.fM = pM;
  }
  
  @Override
  public Object get(final String parameterName) {
    if ("sn".equals(parameterName)) return this.fSn;
    if ("f".equals(parameterName)) return this.fF;
    if ("m".equals(parameterName)) return this.fM;
    return null;
  }
  
  public SocialNetwork getSn() {
    return this.fSn;
  }
  
  public Man getF() {
    return this.fF;
  }
  
  public Woman getM() {
    return this.fM;
  }
  
  @Override
  public boolean set(final String parameterName, final Object newValue) {
    if (!isMutable()) throw new java.lang.UnsupportedOperationException();
    if ("sn".equals(parameterName) ) {
    	this.fSn = (socialnetwork_base.SocialNetwork) newValue;
    	return true;
    }
    if ("f".equals(parameterName) ) {
    	this.fF = (socialnetwork_base.Man) newValue;
    	return true;
    }
    if ("m".equals(parameterName) ) {
    	this.fM = (socialnetwork_base.Woman) newValue;
    	return true;
    }
    return false;
  }
  
  public void setSn(final SocialNetwork pSn) {
    if (!isMutable()) throw new java.lang.UnsupportedOperationException();
    this.fSn = pSn;
  }
  
  public void setF(final Man pF) {
    if (!isMutable()) throw new java.lang.UnsupportedOperationException();
    this.fF = pF;
  }
  
  public void setM(final Woman pM) {
    if (!isMutable()) throw new java.lang.UnsupportedOperationException();
    this.fM = pM;
  }
  
  @Override
  public String patternName() {
    return "hu.bme.mit.inf.dipterv1.incquery.patterns.containment_socialNetwork_family";
  }
  
  @Override
  public List<String> parameterNames() {
    return Containment_socialNetwork_familyMatch.parameterNames;
  }
  
  @Override
  public Object[] toArray() {
    return new Object[]{fSn, fF, fM};
  }
  
  @Override
  public Containment_socialNetwork_familyMatch toImmutable() {
    return isMutable() ? newMatch(fSn, fF, fM) : this;
  }
  
  @Override
  public String prettyPrint() {
    StringBuilder result = new StringBuilder();
    result.append("\"sn\"=" + prettyPrintValue(fSn) + ", ");
    
    result.append("\"f\"=" + prettyPrintValue(fF) + ", ");
    
    result.append("\"m\"=" + prettyPrintValue(fM)
    );
    return result.toString();
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fSn == null) ? 0 : fSn.hashCode());
    result = prime * result + ((fF == null) ? 0 : fF.hashCode());
    result = prime * result + ((fM == null) ? 0 : fM.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
    	return true;
    if (!(obj instanceof Containment_socialNetwork_familyMatch)) { // this should be infrequent
    	if (obj == null) {
    		return false;
    	}
    	if (!(obj instanceof IPatternMatch)) {
    		return false;
    	}
    	IPatternMatch otherSig  = (IPatternMatch) obj;
    	if (!specification().equals(otherSig.specification()))
    		return false;
    	return Arrays.deepEquals(toArray(), otherSig.toArray());
    }
    Containment_socialNetwork_familyMatch other = (Containment_socialNetwork_familyMatch) obj;
    if (fSn == null) {if (other.fSn != null) return false;}
    else if (!fSn.equals(other.fSn)) return false;
    if (fF == null) {if (other.fF != null) return false;}
    else if (!fF.equals(other.fF)) return false;
    if (fM == null) {if (other.fM != null) return false;}
    else if (!fM.equals(other.fM)) return false;
    return true;
  }
  
  @Override
  public Containment_socialNetwork_familyQuerySpecification specification() {
    try {
    	return Containment_socialNetwork_familyQuerySpecification.instance();
    } catch (IncQueryException ex) {
     	// This cannot happen, as the match object can only be instantiated if the query specification exists
     	throw new IllegalStateException (ex);
    }
  }
  
  /**
   * Returns an empty, mutable match.
   * Fields of the mutable match can be filled to create a partial match, usable as matcher input.
   * 
   * @return the empty match.
   * 
   */
  public static Containment_socialNetwork_familyMatch newEmptyMatch() {
    return new Mutable(null, null, null);
  }
  
  /**
   * Returns a mutable (partial) match.
   * Fields of the mutable match can be filled to create a partial match, usable as matcher input.
   * 
   * @param pSn the fixed value of pattern parameter sn, or null if not bound.
   * @param pF the fixed value of pattern parameter f, or null if not bound.
   * @param pM the fixed value of pattern parameter m, or null if not bound.
   * @return the new, mutable (partial) match object.
   * 
   */
  public static Containment_socialNetwork_familyMatch newMutableMatch(final SocialNetwork pSn, final Man pF, final Woman pM) {
    return new Mutable(pSn, pF, pM);
  }
  
  /**
   * Returns a new (partial) match.
   * This can be used e.g. to call the matcher with a partial match.
   * <p>The returned match will be immutable. Use {@link #newEmptyMatch()} to obtain a mutable match object.
   * @param pSn the fixed value of pattern parameter sn, or null if not bound.
   * @param pF the fixed value of pattern parameter f, or null if not bound.
   * @param pM the fixed value of pattern parameter m, or null if not bound.
   * @return the (partial) match object.
   * 
   */
  public static Containment_socialNetwork_familyMatch newMatch(final SocialNetwork pSn, final Man pF, final Woman pM) {
    return new Immutable(pSn, pF, pM);
  }
  
  private static final class Mutable extends Containment_socialNetwork_familyMatch {
    Mutable(final SocialNetwork pSn, final Man pF, final Woman pM) {
      super(pSn, pF, pM);
    }
    
    @Override
    public boolean isMutable() {
      return true;
    }
  }
  
  private static final class Immutable extends Containment_socialNetwork_familyMatch {
    Immutable(final SocialNetwork pSn, final Man pF, final Woman pM) {
      super(pSn, pF, pM);
    }
    
    @Override
    public boolean isMutable() {
      return false;
    }
  }
}