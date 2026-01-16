package org.embulk.spi.time;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import org.junit.BeforeClass;
import org.junit.Test;
import org.jruby.RubyInstanceConfig;
import org.jruby.embed.ScriptingContainer;
import org.jruby.embed.LocalContextScope;
import org.jruby.embed.LocalVariableBehavior;

/**
 * Tests RubyTimeParser.
 */
public class TestRubyTimeParser {
    @Test
    public void testWithMatzRubyTests() throws IOException {
        final ScriptingContainer jruby = loadJRubyScriptingContainerWithMonkeyPatch();

        // Require from Java resource.
        jruby.runScriptlet("require 'ruby/test/date/test_date_strptime.rb'");

        // "test_sz" is intentionally skipped because it fails on JRuby even without the monkey patch.
        assertTrue((Boolean) jruby.runScriptlet(
                "Test::Unit::AutoRunner.run(false, nil, ['--name=/\\A(?!test_sz\\Z).*\\Z/'])"));
    }

    public static ScriptingContainer loadJRubyScriptingContainerWithMonkeyPatch() throws IOException {
        final ScriptingContainer jruby =
                new ScriptingContainer(LocalContextScope.SINGLETHREAD, LocalVariableBehavior.PERSISTENT);

        final RubyInstanceConfig config = jruby.getProvider().getRubyInstanceConfig();
        final String[] arguments = { "--debug" };
        config.processArguments(arguments);

        jruby.runScriptlet("require 'date'");
        jruby.runScriptlet("require 'test/unit'");
        jruby.runScriptlet(
                TestRubyTimeParser.class.getClassLoader().getResourceAsStream("date_monkey_patch.rb"),
                "date_monkey_patch.rb");
        return jruby;
    }
}