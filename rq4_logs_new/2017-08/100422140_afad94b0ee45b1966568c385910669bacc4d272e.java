/**
 * Auto Generated Java Class.
 */
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Stack;
import java.util.Random;
  public class ExtraCards extends Player{

 public ExtraCards(Card[] cards){this.hand = new ArrayList<Card>(Arrays.asList(cards));}
 
 public ArrayList<ArrayList<Integer>> checkPowerCards(ArrayList<Card> hand)
 { int i=0;
   ArrayList<ArrayList<Integer>> powerCards=new ArrayList<ArrayList<Integer>>();
   ArrayList<Integer> cardTwo=new ArrayList<Integer>();
   ArrayList<Integer> cardFour=new ArrayList<Integer>();
   ArrayList<Integer> cardSeven=new ArrayList<Integer>();
   ArrayList<Integer> cardEight=new ArrayList<Integer>();
   for(i=0;i<hand.size();i++){
     if(hand.get(i).rank.equals(Card.RANKS[2]))
     {cardTwo.add(i);}
     if(hand.get(i).rank.equals(Card.RANKS[4]))
     {cardFour.add(i);}
     if(hand.get(i).rank.equals(Card.RANKS[7]))
     {cardSeven.add(i);}
     if(hand.get(i).rank.equals(Card.RANKS[8]))
     { cardEight.add(i);}
    
 }powerCards.add(cardTwo);
   powerCards.add(cardFour);
   powerCards.add(cardSeven);
   powerCards.add(cardEight);
    return powerCards;
 }
 public boolean isPowerCards(Card card){
 if(card.rank.equals(Card.RANKS[2])||card.rank.equals(Card.RANKS[4])||card.rank.equals(Card.RANKS[7])||card.rank.equals(Card.RANKS[8]))
 {
   return true;
 }
  return false;
 }
                                                                                       
 public int getIndex(ArrayList<Player> players){
   int index=0;
   for(int i=0;i<players.size();i++)
   {
     if(this.hand==players.get(i).hand){index=i; return index;}  
   }
   return -1;
 }
  public boolean play(DiscardPile       discardPile, 
                     Stack<Card>       drawPile, 
                     ArrayList<Player> players)
 { int indexOfPlayer=getIndex(players);
   int flag=1;
   if(players.get(indexOfPlayer+1).hand.size()>1){
   Card card=discardPile.top();
   ArrayList<Integer> index=new ArrayList<Integer>();
   Random r=new Random();
   
   ArrayList<Integer> eight=new ArrayList<Integer>();
   for(int i=0;i<this.hand.size();i++)
   { 
     eight=checkEight(this.hand);
     if(this.hand.get(i).compareTo(card)==0)
     {
       index.add(i);
     }
   }
   if(index.size()>0)
   {
     int r2=r.nextInt(index.size());
     int a=index.get(r2);
     discardPile.add(this.hand.remove(a));
   }
   else{
     if(eight.size()>=1)  
     { int j=r.nextInt(eight.size());
       int a=eight.get(j);
       discardPile.add(this.hand.remove(a)); 
     }
     else
     {
       takeCards(drawPile);
     }
   }
   }
    else
    { flag=0;
      for(int i=0;i<4;i++)
      {if(checkPowerCards(this.hand).get(i).size()>0){
        flag=1;
        if(checkPowerCards(this.hand).get(0).size()>0)
        {discardPile.add(this.hand.remove((int)checkPowerCards(this.hand).get(0).get(0)));
         break;
         }
        if(checkPowerCards(this.hand).get(1).size()>0)
        {
         discardPile.add(this.hand.remove((int)checkPowerCards(this.hand).get(1).get(0)));break;
        }
        if(checkPowerCards(this.hand).get(2).size()>0)
        {
         discardPile.add(this.hand.remove((int)checkPowerCards(this.hand).get(2).get(0)));break;
        }
        if(checkPowerCards(this.hand).get(3).size()>0)
        {
         discardPile.add(this.hand.remove((int)checkPowerCards(this.hand).get(3).get(0)));break;
        }
      }
      }
      if(flag==0)
      {
              while(true){
         if(isPowerCards(takeCards(drawPile))==true)
          {break;}
         else{takeCards(drawPile);}
     }
      }
      
    }
   
   
   
   
   
   
  if(this.hand.size() == 0 )
     {return true;}
  return false;
 }
  }
  /* ADD YOUR CODE HERE */
  
