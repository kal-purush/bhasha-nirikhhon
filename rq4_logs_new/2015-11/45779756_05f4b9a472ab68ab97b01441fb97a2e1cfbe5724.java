package codingtests.incrementalsearch;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class IncrementalSearchTest {

    @Test
    @Ignore
    public void testBasicSearch(){
        IncrementalSearch search = new IncrementalSearchImpl(Arrays.asList("ABDE","ABCE","BDAF","BDEC","ABCD","ADED","BCAD","DAEB"));
        assertEquals(Arrays.asList("ABDE","ABCE","BDAF","BDEC","ABCD","ADED","BCAD","DAEB"),search.search(""));   // ""
        assertEquals(Arrays.asList("ABDE","ABCE","ABCD","ADED"),search.search("A"));  // "A"
        assertEquals(Arrays.asList("ABDE","ABCE","ABCD"),search.search("B"));         // "AB"
        assertEquals(Arrays.asList("ABCE","ABCD"),search.search("C"));                // "ABC"
        assertEquals(Arrays.asList("ABCD"),search.search("D"));                       // "ABCD"
        assertEquals(Arrays.asList("ABCE","ABCD"),search.search("\b"));               // backspace -> "ABC"
        assertEquals(Arrays.asList("ABDE","ABCE","ABCD"),search.search("\b"));        // backspace -> "AB"
        assertEquals(Arrays.asList("ABDE","ABCE","ABCD","ADED"),search.search("A"));  // backspace -> "A"
        assertEquals(Arrays.asList("ABDE","ABCE","ABCD","ADED"),search.search("A"));  // backspace -> ""
        assertEquals(Arrays.asList("ABDE","ABCE","BDAF","BDEC","ABCD","ADED","BCAD","DAEB"),search.search("\b")); // backspace -> ""
    }
}