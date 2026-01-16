package de._125m125.kt.ktapi.core.users;

public abstract class AbstractTokenUser<T extends AbstractTokenUser<T>> extends User<T> {

    protected final String tokenId;
    protected final String token;

    public AbstractTokenUser(final String userId, final String tokenId, final String token) {
        super(userId);
        this.tokenId = tokenId;
        this.token = token;
    }

    public String getTokenId() {
        return this.tokenId;
    }

    public String getToken() {
        return this.token;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.token == null) ? 0 : this.token.hashCode());
        result = prime * result + ((this.tokenId == null) ? 0 : this.tokenId.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TokenUser other = (TokenUser) obj;
        if (this.token == null) {
            if (other.token != null) {
                return false;
            }
        } else if (!this.token.equals(other.token)) {
            return false;
        }
        if (this.tokenId == null) {
            if (other.tokenId != null) {
                return false;
            }
        } else if (!this.tokenId.equals(other.tokenId)) {
            return false;
        }
        return true;
    }

}