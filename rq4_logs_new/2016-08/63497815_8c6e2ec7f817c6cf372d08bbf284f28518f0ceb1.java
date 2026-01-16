package imposto.api;


import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import static org.apache.commons.math3.util.Precision.round;

@Document(collection = "imposto")
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class Imposto {
    @Id
    private UUID id;
    public UUID usuarioId;
    private double inss, segVida, vr, vt, custoTotal;

    public Imposto(UUID usuarioId, Double salario) {
        this.id = UUID.randomUUID();
        this.usuarioId = usuarioId;
        calculaImpostos(salario);
    }

    public void calculaImpostos(Double salario) {
        setInss(salario);
        setSegVida(salario);
        setVt(salario);
        setVr(salario);
        setCustoTotal(salario);
    }

    public void setInss(Double salario) {
        this.inss = round(salario * 0.11, 2);
    }

    public void setSegVida(Double salario) {
        this.segVida = round(salario * 0.20, 2);
    }
    public void setVr(Double salario) {
        this.vr = round(salario * 0.13, 2);
    }
    public void setVt(Double salario) {
        this.vt = round(salario * 0.06, 2);
    }

    public void setCustoTotal(Double salario){
        this.custoTotal = round(salario + getCustoTotal() + getInss() + getSegVida() + getVr() + getVt(), 2);
    }
}