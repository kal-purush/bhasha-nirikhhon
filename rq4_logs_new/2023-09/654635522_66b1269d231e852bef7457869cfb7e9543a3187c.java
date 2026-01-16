public class prob13 {
    
    //problem 13: Friends Pairing Problem

//Given n friends, each one can remain single or can be paired up with some other friend. Each friend can be paired only once. Find out the total number of ways in which friends can remain single or can be paired up.
public static int friendsPairing(int n) {
    if (n == 1 || n == 2) {
        return n;
    }

    return friendsPairing(n-1) + (n-1) * friendsPairing(n-2);
}

public static void main(String args[]) {
    System.out.println(friendsPairing(3));
}
}