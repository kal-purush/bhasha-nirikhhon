package usuario.api;


import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Document(collection = "usuario")
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class Usuario {
    @Id
    private UUID id;
    public String nome;
    public double salario;

    public Usuario(String nome, Double salario) {
        this.id = UUID.randomUUID();
        this.nome = nome;
        this.salario = salario;
 }



    public void setNome(String nome) {
        this.nome = nome;
    }

    public void setSalario(Double salario) {
        this.salario = salario;
    }


}