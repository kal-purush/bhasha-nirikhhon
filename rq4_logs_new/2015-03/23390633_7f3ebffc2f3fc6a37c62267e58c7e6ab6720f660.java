package dk.nineconsult.jensen;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.LinkedList;
import java.util.List;

/**
 * Build a Jensen feature with specific features
 * @author Rune Molin, rmo@nineconsult.dk
 */
public class JensenBuilder {
	protected final List<Class<? extends Module>> modules = new LinkedList<>();
	protected final List<SerializationFeature> enabledSerializationFeatures = new LinkedList<>();
	protected final List<SerializationFeature> disabledSerializationFeatures = new LinkedList<>();
	protected final List<String> allowedPackages = new LinkedList<>();
	protected final List<String> deniedPackages = new LinkedList<>();

	public JensenBuilder() {
		enabledSerializationFeatures.add(SerializationFeature.INDENT_OUTPUT);
		disabledSerializationFeatures.add(SerializationFeature.FAIL_ON_EMPTY_BEANS);
	}

	public Jensen build() {
		return new Jensen(this);
	}

	public JensenBuilder registerJacksonModule(Class<? extends Module> module) {
		if (!modules.contains(module)) {
			modules.add(module);
		}
		return this;
	}

	public JensenBuilder enableSerializationFeature(SerializationFeature feature) {
		if (!enabledSerializationFeatures.contains(feature)) {
			enabledSerializationFeatures.add(feature);
		}
		return this;
	}

	public JensenBuilder disableSerializationFeature(SerializationFeature feature) {
		if (!disabledSerializationFeatures.contains(feature)) {
			disabledSerializationFeatures.add(feature);
		}
		return this;
	}

	public JensenBuilder allowPackage(String pck) {
		if (!allowedPackages.contains(pck)) {
			allowedPackages.add(pck);
		}

		return this;
	}


}