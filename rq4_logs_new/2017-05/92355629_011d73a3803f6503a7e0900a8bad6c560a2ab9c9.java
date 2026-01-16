package models;

public class LogAnalyzer
{
    // Where to calculate the hourly access counts.
    private int[] hourCounts;
    // Use a LogfileReader to access the data.
    private LogfileReader reader;
    
    /**
     * Create an object to analyze hourly web accesses.
     */
    public LogAnalyzer()
    { 
        // Create the array object to hold the hourly
        // access counts.
        hourCounts = new int[24];
        // Create the reader to obtain the data.
        reader = new LogfileReader();
    }

    /**
     * Analyze the hourly access data from the log file.
     */
    public void analyzeHourlyData()
    {
        while(reader.hasMoreEntries()) {
            LogEntry entry = reader.nextEntry();
            int hour = entry.getHour();
            hourCounts[hour]++;
        }
    }

    /**
     * Print the hourly counts.
     * These should have been set with a prior
     * call to analyzeHourlyData.
     */
    public void printHourlyCounts()
    {
        System.out.println("Hr: Count");
        for(int hour = 0; hour < hourCounts.length; hour++) {
            System.out.println(hour + ": " + hourCounts[hour]);
        }
    }
    
    /**
     * Print the lines of data read by the LogfileReader
     */
    public void printData()
    {
        reader.printData();
    }
    /**
         * Return the number of accesses recorded in the log
         * file.
         */
    public int numberOfAccesses()
    {
            int total = 0;
            // Add the value in each element of hourCounts
            // to total.
            for(int hour = 0; hour < hourCounts.length; hour++) {
            total = total + hourCounts[hour];
            }
            return total;
    }
      
    public int busiestHour()
    {
            int numberOfAccessesAtBusiest = 0;
            int busiestHour = 0;
            int index = 0;
            
            while(index < hourCounts.length - 1){
                if (numberOfAccessesAtBusiest < hourCounts[index]){
                    busiestHour = index;
                    numberOfAccessesAtBusiest = hourCounts[index];
                    index++;
                }
                else {
                    index++;
                }
            }
            return busiestHour;
    }
    
    public int quietestHour()
    {
            int numberOfAccessesAtQuietest = 9999;
            int quietestHour = 0;
            int index = 0;
            
            while(index < hourCounts.length - 1){
                if (numberOfAccessesAtQuietest > hourCounts[index]){
                    quietestHour = index;
                    numberOfAccessesAtQuietest = hourCounts[index];
                    index++;
                }
                else {
                    index++;
                }
            }
            return quietestHour;
    }
    
    public int twobusiestHour()
    {
            int numberOfAccessesAtBusiest = 0;
            int busiestHour = 0;
            int index = 0;
            
            while(index < hourCounts.length - 1){
                if (numberOfAccessesAtBusiest < hourCounts[index] + hourCounts[index + 1]){
                    busiestHour = index;
                    numberOfAccessesAtBusiest = hourCounts[index] + hourCounts[index + 1];
                    index++;
                }
                else {
                    index++;
                }
            }
            return busiestHour;
    }          
}