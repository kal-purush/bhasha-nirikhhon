using System.Data;

namespace Monitoring.Models.MonitoringModule;

public class MonitoringSystem
{
    public Scheduler scheduler { get; set; }
    public IDbConnection dbConnection { get; set; }
    private CheckerFactory checkerFactory;
    
    public MonitoringSystem(IDbConnection dbConnection)
    {
        this.dbConnection = dbConnection;
        checkerFactory = new CheckerFactory(dbConnection);
        scheduler = new Scheduler();
    }
    
    public void addUrl(Website website, int interval,string checkerKey)
    {
        MonitoringJob job = new MonitoringJob();
        job.websiteId = website;
        job.interval = interval;
        job.checker = checkerFactory.CreateChecker(checkerKey);
        scheduler.scheduleJob(job);
    }

    public void removeUrl(Website website)
    {
        
    }
    
    public void stopJob(Website website , int interval,string checkerKey)
    {
        MonitoringJob job = new MonitoringJob();
        job.websiteId = website;
        job.interval = interval;
        job.checker = checkerFactory.CreateChecker(checkerKey);
        scheduler.stopJob(job);
    }

    public void startMonitoring()
    {
        scheduler.start();
    }
    
    public void stopMonitoring()
    {
        scheduler.stop();
    }
}