package password.vault.api;

import java.util.Objects;

public class CredentialIdentifierDTO {
    private String website;
    private String usernameForWebsite;

    public CredentialIdentifierDTO(String website, String usernameForWebsite) {
        this.website = website;
        this.usernameForWebsite = usernameForWebsite;
    }

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }

    public String getUsernameForWebsite() {
        return usernameForWebsite;
    }

    public void setUsernameForWebsite(String usernameForWebsite) {
        this.usernameForWebsite = usernameForWebsite;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CredentialIdentifierDTO that = (CredentialIdentifierDTO) o;
        return Objects.equals(website, that.website) && Objects.equals(usernameForWebsite, that.usernameForWebsite);
    }

    @Override
    public int hashCode() {
        return Objects.hash(website, usernameForWebsite);
    }

    @Override
    public String toString() {
        return "CredentialIdentifierDTO{" +
                "website='" + website + '\'' +
                ", usernameForWebsite='" + usernameForWebsite + '\'' +
                '}';
    }
}