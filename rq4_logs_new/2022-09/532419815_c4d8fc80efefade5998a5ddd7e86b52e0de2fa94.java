package root.Factory.impl;

import root.Factory.SkinFactory;
import root.Product.Button.Button;
import root.Product.Button.impl.SpringButton;
import root.Product.ComboBox.ComboBox;
import root.Product.ComboBox.impl.SpringComboBox;
import root.Product.TextField.TextField;
import root.Product.TextField.impl.SpringTextField;

public class SpringSkinFactory implements SkinFactory {
	public Button createButton() {
		return new SpringButton();
	}

	public TextField createTextField() {
		return new SpringTextField();
	}

	public ComboBox createComboBox() {
		return new SpringComboBox();
	}
}