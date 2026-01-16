package com.kriscfoster.state;

public class DeliveredState implements PackageState {

    public void next(Package p) {
        p.setState(new ReceivedState());
    }

    public void print() {
        System.out.println("Package delivered to post office, not received yet.");
    }
}