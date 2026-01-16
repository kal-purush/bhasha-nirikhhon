package server;

public class ServiceNotFoundException extends ClassNotFoundException {
    private String serviceName;
    public ServiceNotFoundException(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public String toString() {
        return "ServiceNotFoundException. The service \"" + serviceName + "\" not found";
    }
}