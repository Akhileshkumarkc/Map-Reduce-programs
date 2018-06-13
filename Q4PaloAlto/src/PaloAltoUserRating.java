import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

public class PaloAltoUserRating extends Configured implements Tool{

    public static final int REVIEW_FILE_POS = 0;
    public static final int BUSINESS_FILE_POS =1;
    public static final int FINAL_OUTPUT_POS = 2;

    public static final int B_BUSINESSID_COL = 0;
    public static final int B_FULL_ADDRESS_COL = 1;
    public static final int B_CATEGORIES_COL = 2;

    public static final int R_REVIEWID_COL = 0;
    public static final int R_USERID_COL = 1;
    public static final int R_BUSINESS_ID = 2;
    public static final int R_RATING = 3;

    public static final int I_BUSINESS_ID = 0;
    public static final int I_AVG_RATING = 1;




    //Mapper
    public static class UserRatingMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private java.util.Set<String> businessIds = new java.util.HashSet<>();

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] userReviews = value.toString().split("::");
            String businessIdRTab = userReviews[R_BUSINESS_ID];
            String userID = userReviews[R_USERID_COL];
            String ratingID = userReviews[R_RATING];
            for (String businessIdStr : businessIds) {
                    if(businessIdRTab.equals(businessIdStr)){
                        context.write(new Text(userID), new Text(ratingID));
                    }

            }

        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            try {
                System.out.println("enter setup");
                java.net.URI[] businessDataFiles = context.getCacheFiles();
                System.out.println("Reading File at "+
                        businessDataFiles);

                if(businessDataFiles.length == 0){
                    throw new FileNotFoundException("Cache not found");
                }
                readFile(new Path(businessDataFiles[0]));
               }

               catch (Exception e) {
                System.err.println("Exception while reading stop words file:" + e.getMessage());

            }
        }

        private void readFile(Path filepath) {
            try {
                BufferedReader bufferedReader = new BufferedReader(new FileReader((filepath.toString())));
                String dataLine = null;
                //System.out.println("readFile");
                while ((dataLine = bufferedReader.readLine()) != null) {

                    String[] businessData = dataLine.split("::");

                    String businessId = businessData[B_BUSINESSID_COL];
                    String businessAddress = businessData[B_FULL_ADDRESS_COL];

                    if (businessAddress.toLowerCase().contains("palo alto")) {
                     //   System.out.println(businessId);
                        businessIds.add(businessId);
                    }
                }

            } catch (Exception e) {
                System.err.println("Exception while reading stop words file:" + e.getMessage());

            }
        }

    }
    //Reducer

    public static class UserRatingReducer
            extends Reducer<Text, Text, Text, Text>
    {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val:values){
                context.write(key,val);

            }

        }
    }



    // Driver program
    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new PaloAltoUserRating(),args);
        System.exit(exitCode);

        }

        public int run(String args[]) throws Exception{
            Configuration config = new Configuration();
            String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
            //get All args
            if (otherArgs.length != 3) {
                System.err.println("Argument problem");
                System.err.println("PaloAlto <review.csv> <business.csv> <final_output>");
                System.exit(0);
            }
            System.out.println(otherArgs[REVIEW_FILE_POS]);
            System.out.println(otherArgs[BUSINESS_FILE_POS]);
            // create a job with name "Top10BusinessRatingJob"
            Job paUserRatingJob = Job.getInstance(config, "PaloAltoUserRating");
            paUserRatingJob.setJarByClass(PaloAltoUserRating.class);

            paUserRatingJob.setMapperClass(UserRatingMapper.class);
            paUserRatingJob.setReducerClass(UserRatingReducer.class);
            // set output key type
            paUserRatingJob.setOutputKeyClass(Text.class);
            // set output value type
            paUserRatingJob.setOutputValueClass(Text.class);

            //set the HDFS path of the input data
            FileInputFormat.addInputPath(paUserRatingJob, new Path(otherArgs[REVIEW_FILE_POS]));
            // set the HDFS path for the temporary output file
            FileOutputFormat.setOutputPath(paUserRatingJob, new Path(otherArgs[FINAL_OUTPUT_POS]));
            paUserRatingJob.addCacheFile(new Path(otherArgs[BUSINESS_FILE_POS]).toUri());

            //Distributed Cache
           // DistributedCache.addCacheFile(new Path(otherArgs[BUSINESS_FILE_POS]).toUri(),paUserRatingJob.getConfiguration());

            return (paUserRatingJob.waitForCompletion(true)?0:1);

        }
    }


