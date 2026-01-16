package py.org.fundacionparaguaya.pspserver.families.dtos;

public class FamilyFilterDTOBuilder {
    private Long applicationId;
    private Long organizationId;
    private Long countryId;
    private Long cityId;
    private String name;
    private boolean isActive;
    private String lastModifiedGt;

    public FamilyFilterDTOBuilder applicationId(Long applicationId) {
        this.applicationId = applicationId;
        return this;
    }

    public FamilyFilterDTOBuilder organizationId(Long organizationId) {
        this.organizationId = organizationId;
        return this;
    }

    public FamilyFilterDTOBuilder countryId(Long countryId) {
        this.countryId = countryId;
        return this;
    }

    public FamilyFilterDTOBuilder cityId(Long cityId) {
        this.cityId = cityId;
        return this;
    }

    public FamilyFilterDTOBuilder name(String name) {
        this.name = name;
        return this;
    }

    public FamilyFilterDTOBuilder isActive(boolean isActive) {
        this.isActive = isActive;
        return this;
    }

    public FamilyFilterDTOBuilder lastModifiedGt(String lastModifiedGt) {
        this.lastModifiedGt = lastModifiedGt;
        return this;
    }

    public FamilyFilterDTO build() {
        return new FamilyFilterDTO(applicationId, organizationId, countryId, cityId, name, isActive, lastModifiedGt);
    }


}