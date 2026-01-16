package j8spec.examples;

import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;

import java.io.File;

import static j8spec.J8Spec.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

@RunWith(J8SpecRunner.class)
public class AssumptionExampleTest {{

    context("Windows", () -> {
        beforeEach(() -> {
            assumeThat(System.getProperty("os.name"), containsString("Windows"));
        });

        it("indicates when a directory exists", () -> {
            assertThat(new File("c:\\").exists(), is(true));
        });

        it("indicates when a directory does not exist", () -> {
            assertThat(new File("c:\\dummy").exists(), is(false));
        });
    });

    context("Linux", () -> {
        beforeAll(() -> {
            assumeThat(System.getProperty("os.name"), containsString("Linux"));
        });

        it("indicates when a directory exists", () -> {
            assertThat(new File("/").exists(), is(true));
        });

        it("indicates when a directory does not exist", () -> {
            assertThat(new File("/dummy").exists(), is(false));
        });
    });
}}