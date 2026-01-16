/**
 * <copyright>
 * </copyright>
 *
 */
package org.applause.lang.applauseDsl;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Navigation Bar Item</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * <ul>
 *   <li>{@link org.applause.lang.applauseDsl.NavigationBarItem#getPosition <em>Position</em>}</li>
 *   <li>{@link org.applause.lang.applauseDsl.NavigationBarItem#getIcon <em>Icon</em>}</li>
 *   <li>{@link org.applause.lang.applauseDsl.NavigationBarItem#getTriggers <em>Triggers</em>}</li>
 * </ul>
 * </p>
 *
 * @see org.applause.lang.applauseDsl.ApplauseDslPackage#getNavigationBarItem()
 * @model
 * @generated
 */
public interface NavigationBarItem extends ModelElement
{
  /**
   * Returns the value of the '<em><b>Position</b></em>' attribute.
   * The literals are from the enumeration {@link org.applause.lang.applauseDsl.Position}.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Position</em>' attribute isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Position</em>' attribute.
   * @see org.applause.lang.applauseDsl.Position
   * @see #setPosition(Position)
   * @see org.applause.lang.applauseDsl.ApplauseDslPackage#getNavigationBarItem_Position()
   * @model
   * @generated
   */
  Position getPosition();

  /**
   * Sets the value of the '{@link org.applause.lang.applauseDsl.NavigationBarItem#getPosition <em>Position</em>}' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>Position</em>' attribute.
   * @see org.applause.lang.applauseDsl.Position
   * @see #getPosition()
   * @generated
   */
  void setPosition(Position value);

  /**
   * Returns the value of the '<em><b>Icon</b></em>' containment reference.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Icon</em>' containment reference isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Icon</em>' containment reference.
   * @see #setIcon(ScalarExpression)
   * @see org.applause.lang.applauseDsl.ApplauseDslPackage#getNavigationBarItem_Icon()
   * @model containment="true"
   * @generated
   */
  ScalarExpression getIcon();

  /**
   * Sets the value of the '{@link org.applause.lang.applauseDsl.NavigationBarItem#getIcon <em>Icon</em>}' containment reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>Icon</em>' containment reference.
   * @see #getIcon()
   * @generated
   */
  void setIcon(ScalarExpression value);

  /**
   * Returns the value of the '<em><b>Triggers</b></em>' containment reference list.
   * The list contents are of type {@link org.applause.lang.applauseDsl.ScalarExpression}.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Triggers</em>' containment reference list isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Triggers</em>' containment reference list.
   * @see org.applause.lang.applauseDsl.ApplauseDslPackage#getNavigationBarItem_Triggers()
   * @model containment="true"
   * @generated
   */
  EList<ScalarExpression> getTriggers();

} // NavigationBarItem