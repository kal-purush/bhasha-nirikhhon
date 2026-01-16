package be.mathiasbosman.vim.domain;

import java.time.LocalDateTime;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import lombok.Getter;
import lombok.Setter;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.format.annotation.DateTimeFormat.ISO;

@Getter
@Setter
@MappedSuperclass
public abstract class AbstractDeletableEntity extends AbstractAuditedEntity {

  @Column
  private boolean deleted = Boolean.FALSE;

  @DateTimeFormat(iso = ISO.DATE_TIME)
  private LocalDateTime deletedAt;
}