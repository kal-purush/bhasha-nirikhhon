package hello.domain;

import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Victor on 26-Jan-17.
 */
@Entity
@Table(name = "catw_instance")
public class CatwInstance {

    private Integer catwInstanceId;
    private Integer processId;
    private Integer port;
    private String database;
    private String folderRootLocation;
    private Integer memoryUsage;
    private Server server;
    private Set<App> apps = new HashSet<App>();

    public CatwInstance(){}

    @Id
    @GeneratedValue
    @Column(name = "catw_instance_id")
    public Integer getCatwInstanceId() {
        return catwInstanceId;
    }

    public void setCatwInstanceId(Integer catwInstanceId) {
        this.catwInstanceId = catwInstanceId;
    }

    @Column(name = "process_id")
    public Integer getProcessId() {
        return processId;
    }

    public void setProcessId(Integer processId) {
        this.processId = processId;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    @Column(name = "folder_root_location")
    public String getFolderRootLocation() {
        return folderRootLocation;
    }

    public void setFolderRootLocation(String folderRootLocation) {
        this.folderRootLocation = folderRootLocation;
    }

    @Column(name = "memory_usage")
    public Integer getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(Integer memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    @ManyToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "server_id")
    @LazyCollection(LazyCollectionOption.FALSE)
    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    @OneToMany(cascade = CascadeType.ALL, mappedBy = "catw")
    public Set<App> getApps() {
        return apps;
    }

    public void setApps(Set<App> apps) {
        this.apps = apps;
    }

    public void addApps(App app) {
        this.apps.add(app);
    }
}