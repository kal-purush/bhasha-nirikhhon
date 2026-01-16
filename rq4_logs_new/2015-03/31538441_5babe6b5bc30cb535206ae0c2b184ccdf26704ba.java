package org.arquillian.jruby.embedded;

import org.jboss.arquillian.core.spi.event.Event;

/**
 * This event is fired to signal that Ruby scripts declared via org.arquillian.jruby.RubyScript should be
 * executed.
 */
public class JRubyScriptExecution implements Event {
}