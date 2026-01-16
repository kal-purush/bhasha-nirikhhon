package ca.telagene.exception;

import org.apache.maven.plugin.MojoExecutionException;

public class SampleMojoExecutionException extends MojoExecutionException {

  public SampleMojoExecutionException(final String message) {
    super(message);
  }
}