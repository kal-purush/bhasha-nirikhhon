package ifsc.poo.atendimento;

public class Relatorio {
    private RegistroFila registroFila;

    public void setRegistroFila(RegistroFila registroFila) {
        this.registroFila = registroFila;
    }

    public String relatorioSolicit() {
        StringBuilder builder = new StringBuilder();
        builder.append("Total registradas: ").append(registroFila.getTotalRegistradas()).append("\n");
        builder.append("Atendidas: ").append(registroFila.getTotalAtendidas()).append("\n");
        builder.append("Em espera: ").append(registroFila.getTotalEmEspera()).append("\n");

        if (registroFila.getTotalRegistradas() !=
                (registroFila.getTotalAtendidas() + registroFila.getTotalEmEspera())) {
            builder.append("Contagem de solicitações errada\n");
        }

        return builder.toString();
    }

    public String percentCategoria() {
        StringBuilder builder = new StringBuilder();
        builder.append("\n=== DISTRIBUIÇÃO POR CATEGORIA ===\n");

        int total = registroFila.getTotalRegistradas();
        if (total == 0) {
            builder.append("Não há solicitações registradas.");
            return builder.toString();
        }

        int tec = 0, fin = 0, inf = 0;
        for (Solicitacao s : registroFila.getSolicitacoesAtendidas()) {
            switch (s.getCategoria()) {
                case TECNICO: tec++; break;
                case FINANCEIRO: fin++; break;
                case INFORMACAO: inf++; break;
            }
        }

        for (Solicitacao s : registroFila.getFilaEspera()) {
            switch (s.getCategoria()) {
                case TECNICO: tec++; break;
                case FINANCEIRO: fin++; break;
                case INFORMACAO: inf++; break;
            }
        }

        builder.append(String.format("Suporte Técnico: %.1f%% (%d)\n", (tec * 100f) / total, tec));
        builder.append(String.format("Atendimento Financeiro: %.1f%% (%d)\n", (fin * 100f) / total, fin));
        builder.append(String.format("Informação: %.1f%% (%d)\n", (inf * 100f) / total, inf));

        return builder.toString();
    }
}