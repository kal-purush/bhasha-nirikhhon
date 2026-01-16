package ch.so.agi.simi.entity;

import com.haulmont.chile.core.annotations.Composition;
import com.haulmont.chile.core.annotations.NamePattern;
import com.haulmont.cuba.core.entity.annotation.OnDelete;
import com.haulmont.cuba.core.global.DeletePolicy;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.List;

@NamePattern("%s (_PS)|identifier")
@PrimaryKeyJoinColumn(name = "ID", referencedColumnName = "ID")
@Table(name = "SIMI_PRODUCT_SET")
@Entity(name = "simi_ProductSet")
public class ProductSet extends DataProduct {
    private static final long serialVersionUID = 7972395042416963924L;

    @NotNull
    @Column(name = "IDENTIFIER", nullable = false)
    protected String identifier;

    @Composition
    @OnDelete(DeletePolicy.CASCADE)
    @OneToMany(mappedBy = "productset")
    protected List<PropertiesInList> singleActorList;

    public List<PropertiesInList> getSingleActorList() {
        return singleActorList;
    }

    public void setSingleActorList(List<PropertiesInList> singleActorList) {
        this.singleActorList = singleActorList;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }
}