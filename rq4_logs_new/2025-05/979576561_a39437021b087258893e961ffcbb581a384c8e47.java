package software.sebastian.oposiciones.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

@Entity
@Table(name = "SUSCRIPCION_ETIQUETA")
@IdClass(SuscripcionEtiqueta.PrimaryKey.class)
public class SuscripcionEtiqueta {

    @Id
    @Column(nullable = false)
    private Integer suscripcionId;

    @Id
    @Column(nullable = false)
    private Integer etiquetaId;



    @CreationTimestamp
    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;
    public SuscripcionEtiqueta() {
    }

    public SuscripcionEtiqueta(Integer suscripcionId, Integer etiquetaId ) {
        this.suscripcionId = suscripcionId;
        this.etiquetaId = etiquetaId;
    }

    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public Integer getSuscripcionId() {
        return suscripcionId;
    }

    public void setSuscripcionId(Integer suscripcionId) {
        this.suscripcionId = suscripcionId;
    }

    public Integer getEtiquetaId() {
        return etiquetaId;
    }

    public void setEtiquetaId(Integer etiquetaId) {
        this.etiquetaId = etiquetaId;
    }


    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SuscripcionEtiqueta)) return false;
        SuscripcionEtiqueta that = (SuscripcionEtiqueta) o;
        return Objects.equals(suscripcionId, that.suscripcionId) &&
               Objects.equals(etiquetaId, that.etiquetaId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(suscripcionId, etiquetaId);
    }

    /**
     * Clase de clave primaria compuesta para Suscripcion_Etiqueta.
     */
    public static class PrimaryKey implements Serializable {
        private Integer suscripcionId;
        private Integer etiquetaId;

        public PrimaryKey() {
        }

        public PrimaryKey(Integer suscripcionId, Integer etiquetaId) {
            this.suscripcionId = suscripcionId;
            this.etiquetaId = etiquetaId;
            
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PrimaryKey)) return false;
            PrimaryKey that = (PrimaryKey) o;
            return Objects.equals(suscripcionId, that.suscripcionId) &&
                   Objects.equals(etiquetaId, that.etiquetaId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(suscripcionId, etiquetaId);
        }
    }
}