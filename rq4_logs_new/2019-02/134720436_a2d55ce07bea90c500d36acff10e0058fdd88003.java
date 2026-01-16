package org.yipuran.gsonhelper;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
/**
 * シリアライズ除外アノテーション.
 * <PRE>
 * JSON作成対象のフィールドに付与して、付与したフィールドをJSONシリアライズ対象外にする。
 * 本アノテーションを有効にする為に、GsonBuilder の addSerializationExclusionStrategyで ExcludeWithAnotateStrategy インスタンスを
 * 指定する。
 *      new GsonBuilder().addSerializationExclusionStrategy( new ExcludeWithAnotateStrategy() ).build();
 *
 * ＠Expose の指定による excludeFieldsWithoutExposeAnnotation() と併用した場合、
 * この 除外アノテーションの方が優先され、＠Expose の意味がなくなる。
 * </PRE>
 * @since 4.5
 */
public @interface Exclude{
}