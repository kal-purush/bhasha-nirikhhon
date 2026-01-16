import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;



public class Banca {

    private static ArrayList<Bonifico> listaMovimenti = new ArrayList<Bonifico>();

    public static void operazioneBonifico(ContoCorrente ordinante, ContoCorrente beneficiario,
                                          BigDecimal importo, String causale) {

        if (ordinante.getSaldo().compareTo(importo) < 0) {
            System.out.println("Fondi insufficienti");
        } else {
            ordinante.prelievo(importo);
            beneficiario.versamento(importo);
            Bonifico bonifico = new Bonifico(beneficiario.getIntestatario(), ordinante.getIntestatario(),
                    beneficiario.getIban(), ordinante.getIban(), Valuta.EUR, importo, causale, OffsetDateTime.now());
            listaMovimenti.add(bonifico);
            beneficiario.aggiornaListaMovimenti(bonifico);
            ordinante.aggiornaListaMovimenti(bonifico);
        }
    }

    public static void stampaListaMovimenti() {

        System.out.printf("%-20s %-20s %-20s %-20s %-15s %-20s %-30s %-20s", "Beneficiario", "Ordinante", "IBAN Beneficiario", "IBAN Ordinante",
                                                                            "Divisa", "Importo", "Causale", "Data ordine");
        System.out.println();
        for(Bonifico bonifico : listaMovimenti) {
            System.out.printf("%-20s %-20s %-20s %-20s %-15s %-20.2f %-30s %-20s", bonifico.getBeneficiario(), bonifico.getOrdinante(),
                                                                                    bonifico.getIbanBeneficiario(), bonifico.getIbanOrdinante(),
                                                                                    bonifico.getDivisa().toString(), bonifico.getImporto().doubleValue(),
                                                                                    bonifico.getCausale(), bonifico.getDataOrdine().toString());
            System.out.println();
        }

    }

//    public static BigDecimal cambioValuta(BigDecimal importo) {
//
//
//    }
}