package info.ro.gadget.atsignconfig.core.definition.member;

import java.lang.reflect.Field;

import info.ro.gadget.atsignconfig.core.AtsignConfig;
import info.ro.gadget.atsignconfig.core.exception.AcWrongClassException;
import info.ro.gadget.atsignconfig.core.instance.setter.FieldSetter;

public interface AcField {
	public static final AcField SINGLE = new SingleConfig();
	public static final AcField ARGLESS = new ArglessConfig();
	//public static final AcField COLLECT = new CollectConfig();
	
	public FieldSetter makeMemberSetter(Class<? extends AtsignConfig> clazz, String name, Field field) throws AcWrongClassException;
}