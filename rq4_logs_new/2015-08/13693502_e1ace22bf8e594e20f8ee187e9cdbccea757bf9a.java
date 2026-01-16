package net.unicoen;

import org.eclipse.xtext.conversion.ValueConverterException;
import org.eclipse.xtext.conversion.impl.AbstractLexerBasedConverter;
import org.eclipse.xtext.nodemodel.INode;

public class MyStringValueConverter extends AbstractLexerBasedConverter<String> {
	@Override
	protected String toEscapedString(String value) {
		return value;
	}

	@Override
	protected void assertValidValue(String value) {
		super.assertValidValue(value);
	}

	@Override
	public String toValue(String string, INode node) throws ValueConverterException {
		return string;
	}

}