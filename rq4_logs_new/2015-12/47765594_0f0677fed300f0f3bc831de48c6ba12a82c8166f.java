package com.ouroboros;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;


/*
 * Job to export a HTable into HFiles for bulk import.
 * A modified copy of org.apache.hadoop.hbase.mapreduce.Export job, accepts the same arguments as Export.    
 * 
 * After the export to HFiles, LoadIncrementalHFiles class can be used to import the HFiles into the HTable.
 * hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /user/hfiles/htable htable
 */
public class ExportToHFiles {
    private static final Log LOG = LogFactory.getLog(ExportToHFiles.class);
    final static String NAME = "exportToHFiles";

    static class ExporterToHFiles extends TableMapper<ImmutableBytesWritable, Cell> {
        /**
         * @param row
         *            The current table row key.
         * @param value
         *            The columns.
         * @param context
         *            The current context.
         * @throws IOException
         *             When something is broken with the data.
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
            try {
                for (Cell cell : value.listCells()) {
                    context.write(row, cell);
                }
            } catch (InterruptedException e) {
                LOG.error(e);
            }
        }
    }

    private static final int MAX_ROWS = 1; // 1 row per minute
    private static final int MAX_KEYS = 4096; // rows are wide!

    private static Scan getConfiguredScanForJob(Configuration conf, String[] args) throws IOException {
        Scan s = new Scan();
        // Optional arguments.
        // Set Scan Versions
        int versions = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        s.setMaxVersions(versions);
        // Set Scan Range
        long startTime = args.length > 3 ? Long.parseLong(args[3]) : 0L;
        long endTime = args.length > 4 ? Long.parseLong(args[4]) : Long.MAX_VALUE;
        s.setTimeRange(startTime, endTime);
        // Set cache blocks
        s.setCacheBlocks(false);

        s.setCaching(MAX_ROWS);
        s.setBatch(MAX_KEYS);

        // Set Scan Column Family
        if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
            s.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
        }
        // Set RowFilter or Prefix Filter if applicable.
        Filter exportFilter = getExportFilter(args);
        if (exportFilter != null) {
            LOG.info("Setting Scan Filter for Export.");
            s.setFilter(exportFilter);
        }

        LOG.info("verisons=" + versions + ", starttime=" + startTime + ", endtime=" + endTime);
        return s;
    }

    private static Filter getExportFilter(String[] args) {
        Filter exportFilter = null;
        String filterCriteria = (args.length > 5) ? args[5] : null;
        if (filterCriteria == null)
            return null;
        if (filterCriteria.startsWith("^")) {
            String regexPattern = filterCriteria.substring(1, filterCriteria.length());
            exportFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(regexPattern));
        } else {
            exportFilter = new PrefixFilter(Bytes.toBytes(filterCriteria));
        }
        return exportFilter;
    }

    public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
        String tableName = args[0];
        Path outputDir = new Path(args[1]);
        Job job = Job.getInstance(conf);
        job.setJobName(NAME + "_" + tableName);
        job.setJarByClass(ExporterToHFiles.class);

        // Set optional scan parameters
        Scan s = getConfiguredScanForJob(conf, args);
        TableMapReduceUtil.initTableMapperJob(tableName, s, ExporterToHFiles.class, null, null, job);
        // No reducers. Just write straight to output files.
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        // Use configureIncrementalLoad as advised for bulk loading
        HFileOutputFormat2.configureIncrementalLoad(job, new HTable(conf, tableName));
        HFileOutputFormat2.setOutputPath(job, outputDir);
        return job;
    }

    private static void usage(final String errorMsg) {
        if (errorMsg != null && errorMsg.length() > 0) {
            System.err.println("ERROR: " + errorMsg);
        }
        System.err.println("Usage: ExportToHFiles [-D <property=value>]* <tablename> <outputdir> [<versions> "
                + "[<starttime> [<endtime>]] [^[regex pattern] or [Prefix] to filter]]\n");
        System.err.println("  Note: -D properties will be applied to the conf used. ");
        System.err.println("  For example: ");
        System.err.println("   -D mapred.output.compress=true");
        System.err.println("   -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec");
        System.err.println("   -D mapred.output.compression.type=BLOCK");
        System.err.println("  Additionally, the following SCAN properties can be specified");
        System.err.println("  to control/limit what is exported..");
        System.err.println("   -D " + TableInputFormat.SCAN_COLUMN_FAMILY + "=<familyName>");
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            usage("Wrong number of arguments: " + otherArgs.length);
            System.exit(-1);
        }
        Job job = createSubmittableJob(conf, otherArgs);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}