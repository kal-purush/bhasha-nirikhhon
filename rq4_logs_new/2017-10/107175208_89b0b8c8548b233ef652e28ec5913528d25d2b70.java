package org.springframework.samples.petclinic;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.tqdev.metrics.core.MetricRegistry;
import com.tqdev.metrics.jmx.JmxReporter;

@Component
public class JmxReporterLoader {

	/**
	 * Start with the default domain (the package name of this class).
	 */
	@Autowired
	public JmxReporterLoader(MetricRegistry metricRegistry) {
		JmxReporter.start("com.tqdev.metrics", metricRegistry);
	}

}