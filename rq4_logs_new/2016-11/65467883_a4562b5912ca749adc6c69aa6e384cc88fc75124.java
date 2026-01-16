package unal.architecture.credits;

public class CreditsWSClient {

    public static String createCreditWS(String name, float quantity, int numberOfPayments) {
        MakeAcquiredProductWS_Service service = new MakeAcquiredProductWS_Service();
        MakeAcquiredProductWS creditsService = service.getMakeAcquiredProductWSPort();

        return creditsService.makeAcquiredProduct(name,quantity,numberOfPayments);
    }

}