package ar.edu.itba.pod.client.util;

import ar.edu.itba.pod.api.OrderStringIntMapCollator;
import ar.edu.itba.pod.client.Client;
import ar.edu.itba.pod.client.DataReader;
import ar.edu.itba.pod.model.Data;
import ar.edu.itba.pod.model.DepartmentNameOcurrenciesCount;
import ar.edu.itba.pod.model.DepartmentPairOcurrenciesCount;
import ar.edu.itba.pod.query1.ProvinceRegionMapper;
import ar.edu.itba.pod.query1.ProvinceRegionReducerFactory;
import ar.edu.itba.pod.query2.Query2Combiner;
import ar.edu.itba.pod.query2.Query2CountCollator;
import ar.edu.itba.pod.query2.Query2CountReducerFactory;
import ar.edu.itba.pod.query2.Query2Mapper;
import ar.edu.itba.pod.query3.UnemploymentIndexCollator;
import ar.edu.itba.pod.query3.UnemploymentIndexMapper;
import ar.edu.itba.pod.query3.UnemploymentIndexReducerFactory;
import ar.edu.itba.pod.query4.HogarCountMapper;
import ar.edu.itba.pod.query4.HogarCountReducerFactory;
import ar.edu.itba.pod.query5.Query5Collator;
import ar.edu.itba.pod.query5.Query5Combiner;
import ar.edu.itba.pod.query5.Query5Mapper;
import ar.edu.itba.pod.query5.Query5ReducerFactory;
import ar.edu.itba.pod.query6.DepartmentCollator;
import ar.edu.itba.pod.query6.DepartmentMapper;
import ar.edu.itba.pod.query6.DepartmentReducerFactory;
import ar.edu.itba.pod.query7.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by marlanti on 11/6/17.
 */
public class QueryUtil {

    private static Logger logger = Client.getLogger();
    private static long startTime;

    /* *********************************************************** */
    /* ************************* QUERY 1 ************************* */
    /* *********************************************************** */

    public static void query1(HazelcastInstance hz, String fileName, String listName) {
        JobTracker jobTracker = hz.getJobTracker(listName);

        IList<Data> list = getList(listName,hz,fileName,null);
        startExecutionTime();
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );

        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new ProvinceRegionMapper())
                .reducer(new ProvinceRegionReducerFactory())
                .submit(new OrderStringIntMapCollator()); // adentro del submit recibe un collator para ordenar

        Map<String, Integer> result = null;
        endQuery("Query 1", future, result);

    }
    public static IList<Data> getList(String listName, HazelcastInstance client, String fileName, String provinceRestriction) {
        IList<Data> list = client.getList(listName);
        list.clear();
        DataReader.readToList(list, fileName, provinceRestriction);
        return list;
    }

    /* *********************************************************** */
    /* ************************* QUERY 2 ************************* */
    /* *********************************************************** */

    public static void query2(HazelcastInstance hz, String fileName, String provinceName, int n) {
        JobTracker jobTracker = hz.getJobTracker("departmentCount");
        IList<Data> list = getQuery2List(hz, fileName, provinceName);

        final KeyValueSource<String, Data> source = KeyValueSource.fromList(list);
        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query2Mapper())
                .combiner(new Query2Combiner())
                .reducer(new Query2CountReducerFactory())
                .submit(new Query2CountCollator(n));

        Map<String, Long> result = null;

        endQuery("Query 2 (with province " + provinceName + " and n " + n + ")", future, result);
    }
    private static IList<Data> getQuery2List(HazelcastInstance client, String fileName, String provinceName) {
        IList<Data> list = client.getList("departments");
        list.clear();
        DataReader.readToList(list, fileName, provinceName);
        startExecutionTime();
        return list;
    }

    /* *********************************************************** */
    /* ************************* QUERY 3 ************************* */
    /* *********************************************************** */

    public static void query3(HazelcastInstance hz, String fileName, String listName) {
        JobTracker jobTracker = hz.getJobTracker(listName);
        IList<Data> list = getList(listName,hz,fileName,null);

        startExecutionTime();
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );


        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Double>> future = job
                .mapper(new UnemploymentIndexMapper())
                .reducer(new UnemploymentIndexReducerFactory())
                .submit(new UnemploymentIndexCollator()); // adentro del submit recibe un collator para ordenar


        Map<String, Double> result = null;
        endQuery("Query 3", future, result);

    }

    /* *********************************************************** */
    /* ************************* QUERY 4 ************************* */
    /* *********************************************************** */

    public static void query4(HazelcastInstance hz, String fileName, String listName) {

        JobTracker jobTracker = hz.getJobTracker(listName);

        IList<Data> list = getList(listName,hz,fileName,null);
        startExecutionTime();
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );


        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new HogarCountMapper())
                .reducer(new HogarCountReducerFactory())
                .submit(new OrderStringIntMapCollator()); // adentro del submit recibe un collator para ordenar

        Map<String, Integer> result = null;
        endQuery("Query 4", future, result);

    }

    /* *********************************************************** */
    /* ************************* QUERY 5 ************************* */
    /* *********************************************************** */

    public static void query5(HazelcastInstance hz, String fileName, String listName) {
        JobTracker jobTracker = hz.getJobTracker(listName);

        IList<Data> list = getList(listName,hz,fileName,null);
        startExecutionTime();
        final KeyValueSource<String, Data> source = KeyValueSource.fromList(list);
        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Float>> future = job
                .mapper(new Query5Mapper())
                .combiner(new Query5Combiner())
                .reducer(new Query5ReducerFactory())
                .submit(new Query5Collator());

        Map<String, Float> result = null;
        endQuery("Query 5", future, result);
    }

    /* *********************************************************** */
    /* ************************* QUERY 6 ************************* */
    /* *********************************************************** */

    public static void query6(HazelcastInstance hz, String fileName, int n, String listName) {

        JobTracker jobTracker = hz.getJobTracker(listName);
        IList<Data> list = getList(listName,hz,fileName,null);
        startExecutionTime();
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );


        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, DepartmentNameOcurrenciesCount>> future = job
                .mapper(new DepartmentMapper())
                .reducer(new DepartmentReducerFactory())
                .submit(new DepartmentCollator(n)); // adentro del submit recibe un collator para ordenar

        Map<String, DepartmentNameOcurrenciesCount> result = null;
        endQuery("Query 6 with n " + n, future, result);
    }

    /* *********************************************************** */
    /* ************************* QUERY 7 ************************* */
    /* *********************************************************** */

    public static void query7(HazelcastInstance hz, String fileName, int n, String listName) {

        JobTracker jobTracker = hz.getJobTracker(listName);

        IList<Data> list = getList(listName,hz,fileName,null);
        startExecutionTime();
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );

        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, DepartmentPairOcurrenciesCount>> future = job
                .mapper(new DepartmentInProvinceMapper())
                .combiner(new DeparmentPairCombinerFactory())
                .reducer(new DepartmentPairReducerFactory())
                .submit(new DepartmentPairCollator(1)); // adentro del submit recibe un collator para ordenar

        Map<String, DepartmentPairOcurrenciesCount> result = null;
        endQuery("Query 7.1 with n " + n, future, result);
    }


    /* *********************************************************** */
    /* ************************* QUERY 7v2 ************************* */
    /* *********************************************************** */

    public static void query7v2(HazelcastInstance hz, String fileName, int n, String listName) {

        JobTracker jobTracker = hz.getJobTracker(listName);

        IList<Data> list = getList(listName,hz,fileName,null);
        startExecutionTime();
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );


        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Map<String, String>>> future = job
                .mapper(new DepartmentInProvinceMapper())
                .combiner(new DeparmentPairCombinerFactory())
                .reducer(new DepartmentPairReducerFactory())
                .submit(); // adentro del submit recibe un collator para ordenar

        try {
            Map<String, Map<String, String>> result = future.get();
            IMap<String, Map<String, String>> imap = hz.getMap("provincePairInDepartment");
            for (String key : result.keySet()){
                imap.put(key, result.get(key));
            }
            final KeyValueSource<String, Map<String, String>> source1 = KeyValueSource.fromMap( imap );
            Job<String, Map<String, String>> job1 = jobTracker.newJob(source1);
            ICompletableFuture<Map<String, Integer>> future1 = job1
                    .mapper(new ProvPairMapper())
                    .reducer(new ProvPairReducerFactory())
                    .submit(new ProvPairCollator(n));
            Map<String, Integer> result1 = future1.get();
            for (String pairprov : result1.keySet()) {
                logger.info(pairprov +","+ result1.get(pairprov));
            }
        } catch (InterruptedException e1) {

        } catch (ExecutionException e) {

        } finally {
            logExecutionTime("Query 7.2 with n " + n);
        }
    }

    private static void startExecutionTime() {
        startTime = System.nanoTime();
        logger.info("Starting query");
    }
    private static void logExecutionTime(String queryName) {
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        logger.info(queryName + " execution time: " + duration/1000000 + " ms");
    }

    private static void endQuery(String infoLog, ICompletableFuture future, Map result) {
        try {
            result = (Map) future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            logExecutionTime(infoLog);
            logger.info("RESULTS: "+result.toString());
            System.exit(0);
        }
    }
}