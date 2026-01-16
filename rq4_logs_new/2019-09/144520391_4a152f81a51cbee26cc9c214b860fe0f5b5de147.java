package com.somamission.peanutbutter.param;

public class PhotoParams {
  public static class Builder {
    private String profileUrl;
    private String coverUrl;

    public Builder withProfileUrl(String profileUrl) {
      this.profileUrl = profileUrl;
      return this;
    }

    public Builder withCoverUrl(String coverUrl) {
      this.coverUrl = coverUrl;
      return this;
    }

    public PhotoParams build() {
      PhotoParams photoParams = new PhotoParams();
      photoParams.profileUrl = this.profileUrl;
      photoParams.coverUrl = this.coverUrl;

      return photoParams;
    }
  }

  private String profileUrl;
  private String coverUrl;

  public String getProfileUrl() {
    return profileUrl;
  }

  public String getCoverUrl() {
    return coverUrl;
  }
}