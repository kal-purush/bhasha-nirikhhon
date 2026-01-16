package com.avisow.designpatterns.behavioralpatterns.chainofresponsibility;

/// <summary>
/// MainApp startup class for Structural
/// Chain of Responsibility Design Pattern.
///     a way of passing request between a chain of object
/// </summary>
public class App 
{
    public static void main( String[] args )
    {
        // Setup Chain of Responsibility
        Handler h1 = new ConcreteHandler1();
        Handler h2 = new ConcreteHandler2();
        Handler h3 = new ConcreteHandler3();
        h1.setSuccessor(h2);
        h2.setSuccessor(h3);

        // Generate and process request
        int[] requests = { 2, 5, 14, 22, 18, 3, 27, 20 };

        for (int request : requests)
        {
            h1.handleRequest(request);
        }
    }
}

/// <summary>
/// The 'Handler' abstract class
/// </summary>
abstract class Handler
{
    protected Handler successor;

    public void setSuccessor(Handler successor)
    {
        this.successor = successor;
    }

    public abstract void handleRequest(int request);
}

/// <summary>
/// The 'ConcreteHandler1' class
/// </summary>
class ConcreteHandler1 extends Handler {
    @Override
    public void handleRequest(int request) {
        if (request >= 0 && request < 10) {
            System.out.println(this.getClass().toString() + " handled request " + request);
        } else if (successor != null) {
            successor.handleRequest(request);
        }
    }
}

/// <summary>
/// The 'ConcreteHandler2' class
/// </summary>
class ConcreteHandler2 extends Handler {
    @Override
    public void handleRequest(int request) {
        if (request >= 10 && request < 20) {
            System.out.println(this.getClass().toString() + " handled request " + request);
        } else if (successor != null) {
            successor.handleRequest(request);
        }
    }
}

/// <summary>
/// The 'ConcreteHandler3' class
/// </summary>
class ConcreteHandler3 extends Handler {
    @Override
    public void handleRequest(int request) {
        if (request >= 20 && request < 30) {
            System.out.println(this.getClass().toString() + " handled request " + request);
        } else if (successor != null) {
            successor.handleRequest(request);
        }
    }
}