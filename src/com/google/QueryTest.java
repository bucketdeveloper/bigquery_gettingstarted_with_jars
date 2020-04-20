package com.google;
import com.google.cloud.bigquery.*;

import java.util.UUID;

public class QueryTest {

    public static void main(String[] args) {
        System.out.println("Querying BigQuery for StackOverflow questions containing the term 'google-bigquery'...");

        /*
         * Initialize the BigQuery instance
         */
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        /*
         * Create the QueryConfig, which is really just a wrapper for the Query
         */
        long JOB_TIMEOUT_MS = 100000;
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        "SELECT "
                                + "CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING)) as url, "
                                + "view_count "
                                + "FROM `bigquery-public-data.stackoverflow.posts_questions` "
                                + "WHERE tags like '%google-bigquery%' "
                                + "ORDER BY favorite_count DESC LIMIT 10")
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .setJobTimeoutMs(JOB_TIMEOUT_MS)
                        .build();

        /*
         * BigQuery is Job-based. so we create a job to get an ID, then add the Query to that job
         */
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        /*
         * We're going to wait for the job, which we configured to wait for 100s.
         * Because this can result in an interrupt, we will need to catch an InterruptedException
         */
        try {
            long JOB_START_TIME = System.currentTimeMillis();

            queryJob = queryJob.waitFor();

            System.out.println("Query completed in "+(System.currentTimeMillis() - JOB_START_TIME)+" ms");

            // Get the result set after the job completes
            TableResult result = queryJob.getQueryResults();

            /*
             * We iterate over the result set row by row, and get our values out using the column names
             */
            for (FieldValueList row : result.iterateAll()) {
                String url = row.get("url").getStringValue();
                long viewCount = row.get("view_count").getLongValue();
                System.out.printf("url: %s views: %d%n", url, viewCount);
            }
        } catch (InterruptedException ie) {
            System.err.println(ie);
            System.exit(1);
        }

// Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            if (queryJob.getStatus().getError() != null)
                throw new RuntimeException(queryJob.getStatus().getError().toString());

    }

}
