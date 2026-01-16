package br.com.sococo.resumo.domain;

import br.com.sococo.resumo.domain.dto.*;

import java.util.List;

public class Dashboard {
    List<CocoDTO> cocos;
    List<CRIFlococoDTO> criFlococos;
    List<OleoDTO> oleos;
    List<TortaDTO> tortas;
    List<AguaCocoDTO> aguaCocos;
    List<CocoGerminadoDTO> cocoGerminados;
    List<TotalCacambaDTO> totalCacambas;
    List<CaixaPadraoDTO> caixaPadraos;
    List<TotalFardosDTO> totalFardos;

    public Dashboard() {
    }

    public Dashboard(List<CocoDTO> cocos, List<CRIFlococoDTO> criFlococos, List<OleoDTO> oleos, List<TortaDTO> tortas, List<AguaCocoDTO> aguaCocos, List<CocoGerminadoDTO> cocoGerminados, List<TotalCacambaDTO> totalCacambas, List<CaixaPadraoDTO> caixaPadraos, List<TotalFardosDTO> totalFardos) {
        this.cocos = cocos;
        this.criFlococos = criFlococos;
        this.oleos = oleos;
        this.tortas = tortas;
        this.aguaCocos = aguaCocos;
        this.cocoGerminados = cocoGerminados;
        this.totalCacambas = totalCacambas;
        this.caixaPadraos = caixaPadraos;
        this.totalFardos = totalFardos;
    }

    public List<CocoDTO> getCocos() {
        return cocos;
    }

    public void setCocos(List<CocoDTO> cocos) {
        this.cocos = cocos;
    }

    public List<CRIFlococoDTO> getCriFlococos() {
        return criFlococos;
    }

    public void setCriFlococos(List<CRIFlococoDTO> criFlococos) {
        this.criFlococos = criFlococos;
    }

    public List<OleoDTO> getOleos() {
        return oleos;
    }

    public void setOleos(List<OleoDTO> oleos) {
        this.oleos = oleos;
    }

    public List<TortaDTO> getTortas() {
        return tortas;
    }

    public void setTortas(List<TortaDTO> tortas) {
        this.tortas = tortas;
    }

    public List<AguaCocoDTO> getAguaCocos() {
        return aguaCocos;
    }

    public void setAguaCocos(List<AguaCocoDTO> aguaCocos) {
        this.aguaCocos = aguaCocos;
    }

    public List<CocoGerminadoDTO> getCocoGerminados() {
        return cocoGerminados;
    }

    public void setCocoGerminados(List<CocoGerminadoDTO> cocoGerminados) {
        this.cocoGerminados = cocoGerminados;
    }

    public List<TotalCacambaDTO> getTotalCacambas() {
        return totalCacambas;
    }

    public void setTotalCacambas(List<TotalCacambaDTO> totalCacambas) {
        this.totalCacambas = totalCacambas;
    }

    public List<CaixaPadraoDTO> getCaixaPadraos() {
        return caixaPadraos;
    }

    public void setCaixaPadraos(List<CaixaPadraoDTO> caixaPadraos) {
        this.caixaPadraos = caixaPadraos;
    }

    public List<TotalFardosDTO> getTotalFardos() {
        return totalFardos;
    }

    public void setTotalFardos(List<TotalFardosDTO> totalFardos) {
        this.totalFardos = totalFardos;
    }

}