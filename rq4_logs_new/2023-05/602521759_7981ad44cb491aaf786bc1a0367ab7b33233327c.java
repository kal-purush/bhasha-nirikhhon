
package br.senac.sp.projeto.cineticketoficial.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@RequiredArgsConstructor
@Entity
@Table(name = "acesso")
@NamedQueries({
        @NamedQuery(name = "Acesso.findAll", query = "SELECT a FROM Acesso a"),
        @NamedQuery(name = "Acesso.findByEmailCliente", query = "SELECT a FROM Acesso a WHERE a.email = :emailCliente"),
        @NamedQuery(name = "Acesso.findBySenha", query = "SELECT a FROM Acesso a WHERE a.senha = :senha")})
public class Acesso implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @Basic(optional = false)
    @Column(name = "email_cliente")
    private String email;

    @Basic(optional = false)
    @Column(name = "senha")
    private String senha;

    @JsonIgnore
    @JoinColumn(name = "email_cliente", referencedColumnName = "email", insertable = false, updatable = false)
    @OneToOne(optional = false, fetch = FetchType.EAGER)
    private Cliente cliente;

}