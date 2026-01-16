package com.github.ssullivan.guice;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ContainerCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.github.ssullivan.model.aws.AwsS3Settings;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.jupiter.api.Test;

public class AwsS3ClientModuleTest {
  @Test
  public void testCreatingAwsS3Client() {
    final AwsS3Settings awsS3Settings = new AwsS3Settings("test", "test");
    final Injector injector = Guice
        .createInjector(new AwsS3ClientModule(awsS3Settings, "localhost"));
    final AmazonS3 amazonS3 = injector.getInstance(AmazonS3.class);

    final AWSCredentialsProvider credentialProvider = awsS3Settings.getAWSCredentialsProvider();
    MatcherAssert.assertThat(credentialProvider, Matchers.instanceOf(ContainerCredentialsProvider.class));

    MatcherAssert.assertThat(amazonS3, Matchers.notNullValue());
    MatcherAssert.assertThat(amazonS3.getRegionName(), Matchers.equalTo("test"));
    int j = 0;
  }
}