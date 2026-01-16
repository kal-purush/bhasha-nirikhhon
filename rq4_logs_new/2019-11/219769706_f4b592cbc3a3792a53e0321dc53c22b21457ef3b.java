public class SportmasterBonusService {
    public int calculatePoint(int totalCost,int cost)
    {
        if(totalCost==0)
        {
            return 0;
        }
        if(totalCost<15001){
            return 50;
        }
        if(totalCost>150000){
            return 100;
        }
        return 70;
    }
    public int chargeBonus(SportmasterClient client,int cost){
        int point=calculatePoint(client.getTotalCost(),cost);
        client.increaseTotalCost(cost);
        int result=cost/1000*point;
        return result;
    }
}