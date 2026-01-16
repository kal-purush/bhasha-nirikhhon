import java.time.LocalDateTime;
import javax.persistence.Convert;
import no.nav.foreldrepenger.info.felles.datatyper.BehandlingType;

public class Behandling {
    @Column(name = "BEHANDLING_TYPE")
    @Convert(converter = BehandlingType.KodeverdiConverter.class)
    private BehandlingType behandlingType;

    @OneToMany(mappedBy = "behandlingId")
    private List<BehandlingÅrsak> årsaker = new ArrayList<>();

    @Column(name = "opprettet_tid", nullable = false)
    private LocalDateTime opprettetTidspunkt;

    @Column(name = "endret_tid")
    private LocalDateTime endretTidspunkt;

    public List<BehandlingÅrsak> getÅrsaker() {
        return årsaker;
    public BehandlingType getBehandlingType() {
        return behandlingType;
    }

    public LocalDateTime getOpprettetTidspunkt() {
        return opprettetTidspunkt;
    }

    public LocalDateTime getEndretTidspunkt() {
        return endretTidspunkt;
    }

        public Builder medBehandlingÅrsaker(List<BehandlingÅrsak> årsaker) {
            behandling.årsaker = årsaker;
            return this;
        }

        public Builder medBehandlingType(BehandlingType behandlingType) {
            behandling.behandlingType = behandlingType;
            return this;
        }

package no.nav.foreldrepenger.info.domene;

import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.Immutable;

import no.nav.foreldrepenger.info.felles.datatyper.BehandlingÅrsakType;

@Entity(name = "BehandlingÅrsak")
@Table(name = "BEHANDLING_ARSAK")
@Immutable
public class BehandlingÅrsak {

    @Id
    @Column(name = "BEHANDLING_ARSAK_ID")
    private Long id;

    @Column(name = "BEHANDLING_ARSAK_TYPE")
    @Convert(converter = BehandlingÅrsakType.KodeverdiConverter.class)
    private BehandlingÅrsakType type;

    @Column(name = "BEHANDLING_ID")
    private Long behandlingId;

    public BehandlingÅrsak() {
        // Hibernate
    }

    public BehandlingÅrsak(BehandlingÅrsakType type) {
        this.type = type;
    }

    public Long getId() {
        return id;
    }

    public BehandlingÅrsakType getType() {
        return type;
    }

    public Long getBehandlingId() {
        return behandlingId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BehandlingÅrsak that = (BehandlingÅrsak) o;
        return type == that.type && Objects.equals(behandlingId, that.behandlingId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, behandlingId);
    }

    @Override
    public String toString() {
        return "BehandlingÅrsak{" + "type='" + type + '\'' + ", behandlingId=" + behandlingId + '}';
    }
}