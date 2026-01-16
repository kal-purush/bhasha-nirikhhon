package com.example.mi_b_wizard;

import com.example.mi_b_wizard.Data.Card;
import com.example.mi_b_wizard.Data.Colour;
import com.example.mi_b_wizard.Data.Rank;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class CardTest {
    Card card;
    Card card1;
    Card card2;
    Card card3;

    @Before
    public void before(){
        card = new Card(0,1);
        card1 = new Card(9,2);
        card2 = new Card(0,1);
        card3 = new Card(5,3);
    }
    @After
    public void after(){
        card = null;
        card1 = null;
        card2 = null;
    }
    @Test
    public void testGetRank(){
        assertEquals(Rank.NARR,card.getRank());
        assertEquals(Rank.ZAUBERER, card1.getRank());
        assertEquals(Rank.FÜNF, card3.getRank());

    }
    @Test
    public void testSetRankFailed(){
        try{
            card.setRank(10);
            fail();
        }catch (RuntimeException e){
            assertEquals(e.getMessage(),"Rank must be between 0 and 9");

        }
    }
    @Test
    public void testSetRankFailed2(){
        try{
            card.setRank(-2);
            fail();
        }catch (RuntimeException e){
            assertEquals(e.getMessage(),"Rank must be between 0 and 9");

        }
    }
    @Test
    public void testSetRankValide(){
        card.setRank(2);
        assertEquals(Rank.ZWEI,card.getRank());
    }
    @Test
    public void testGetColour(){
        assertEquals(Colour.GRÜN, card.getColour());
        assertEquals(Colour.GELB, card1.getColour());
        assertEquals(Colour.ROT, card3.getColour());
    }
    @Test
    public void testSetColourValide(){
        card.setColour(2);
        assertEquals(Colour.GELB, card.getColour());
    }
    @Test
    public void testSetClourFailed(){
        try{
            card.setColour(5);
            fail();
        }catch(RuntimeException e){
            assertEquals(e.getMessage(), "Colour must be between 0 and 3");
        }
    }
    @Test
    public void testisMagicianTrue(){
        assertTrue(card1.isMagician());
    }
    @Test
    public void testisMagicionFalse(){
        assertFalse(card.isMagician());
        assertFalse(card3.isMagician());
        assertFalse(card2.isMagician());
    }
    @Test
    public void testisNarrTrue(){
        assertTrue(card.isNarr());
        assertTrue(card2.isNarr());
    }
    @Test
    public void testisNarrFalse(){
        assertFalse(card1.isNarr());
        assertFalse(card3.isNarr());
    }
    @Test
    public void testGetId(){
        assertEquals("NARR_GRÜN",card.getId());
    }
    @Test
    public void testSetIdValide(){
        card.setId(9,1);
        assertEquals("ZAUBERER_GRÜN",card.getId());
    }
    @Test
    public void testEqualToOtherCardTrue(){
        assertEquals(true,card.equalToOtherCard(card2));
    }
    @Test
    public void testEqualToOtherCardFalse(){
        assertEquals(false, card.equalToOtherCard(card1));
    }
    @Test
    public void testToString(){
        assertEquals("NARR in GRÜN",card.toString());
    }
}
