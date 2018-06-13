import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class AvTop10Business
{
    public static final int REVIEW_FILE_POS = 0;
    public static final int BUSINESS_FILE_POS =1;
    public static final int TEMP_FILE_POS = 2;
    public static final int FINAL_OUTPUT_POS = 3;

    public static final int B_BUSINESSID_COL = 0;
    public static final int B_FULL_ADDRESS_COL = 1;
    public static final int B_CATEGORIES_COL = 2;

    public static final int R_REVIEWID_COL = 0;
    public static final int R_USERID_COL = 1;
    public static final int R_BUSINESS_ID = 2;
    public static final int R_RATING = 3;

    public static final int I_BUSINESS_ID = 0;
    public static final int I_AVG_RATING = 1;

    public static final String TABLE1 = "T1";
    public static final String TABLE2 = "T2";
    public static final String SEPERATOR = "|";
    public static final String ESC_SEPERATOR = "\\|";



    public static class Top10BusinessRatingMapper
            extends Mapper<LongWritable, Text, Text, Text>
    {
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            String[] userReviews = value.toString().split("::");
            String   businessId = userReviews[R_BUSINESS_ID];
            String   ratingID = userReviews[R_RATING];


            context.write(new Text(businessId), new Text(ratingID));
        }
    }

    public static class Top10BusinessRatingReducer
            extends Reducer<Text, Text, Text, Text>
    {
        private TreeMap<String, Double> ratingTreeMap = new TreeMap();
        private BusRatComparator busRatComparator = new BusRatComparator(this.ratingTreeMap);
        private TreeMap<String, Double> sortedBusRatMap = new TreeMap(this.busRatComparator);

        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            Double sumOfRatings = 0.0;
            Double countOfRatings = 0.0;
            for (Text value : values)
            {
                sumOfRatings += Double.parseDouble(value.toString());
                countOfRatings += 1.0;
            }
            Double avgRat = new Double(sumOfRatings / countOfRatings);
            this.ratingTreeMap.put(key.toString(), Double.valueOf(avgRat));
        }

        class BusRatComparator
                implements Comparator<String>
        {
            TreeMap<String, Double> busRatMap;

            public BusRatComparator(TreeMap<String, Double> treeMap )
            {
                this.busRatMap = treeMap;
            }

            public int compare(String a, String b)
            {
                if (((Double)this.busRatMap.get(a)) >= ((Double)this.busRatMap.get(b))) {
                    return -1;
                }
                return 1;
            }
        }

        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            this.sortedBusRatMap.putAll(this.ratingTreeMap);
            int count = 0;
            for (java.util.Map.Entry<String, Double> entry : this.sortedBusRatMap.entrySet())
            {
                if (count > 10) {
                    context.write(new Text(entry.getKey()), new Text(String.valueOf(entry.getValue())));
                    count++;
                }
                else{
                    break;
                }

            }
        }
    }

    public static class TopTenRatedBusinessRatingMapper
            extends Mapper<LongWritable, Text, Text, Text>
    {
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            String BusAvgRatLine = value.toString().trim();
            String[] BusAvgRatdetail = BusAvgRatLine.split("\t");
            String busID = BusAvgRatdetail[I_BUSINESS_ID].trim();
            String busRat = BusAvgRatdetail[I_AVG_RATING].trim();
            String outKey = busID;
            String outValue =  TABLE1+ SEPERATOR + busID + SEPERATOR + busRat;
            context.write(new Text(outKey), new Text(outValue));
        }
    }

    public static class AllBusinessDetailsMapper
            extends Mapper<LongWritable, Text, Text, Text>
    {
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            String[] businessData = value.toString().split("::");

            String outKey = businessData[B_BUSINESSID_COL].trim();
            String outValues = (TABLE2+SEPERATOR+ businessData[B_BUSINESSID_COL].trim() +
                                 SEPERATOR + businessData[B_FULL_ADDRESS_COL].trim() +
                                 SEPERATOR + businessData[B_CATEGORIES_COL].trim());
            context.write(new Text(outKey), new Text(outValues));
        }
    }

    public static class TopTenRatedBusinessDetailReducer
            extends Reducer<Text, Text, Text, Text>
    {
        private ArrayList<String> topTenBusList = new ArrayList();
        private ArrayList<String> businessDetailsList = new ArrayList();


        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            for (Text text : values)
            {
                String value = text.toString();
                if (value.startsWith(TABLE1)) {
                    this.topTenBusList.add(value.substring(3));
                }
                else {
                    this.businessDetailsList.add(value.substring(3));
                }
            }
        }

        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            for (String topBusiness : this.topTenBusList) {
                for (String detail : this.businessDetailsList)
                {
                    String[] t1Split = topBusiness.split(ESC_SEPERATOR);
                    String t1BusinessId = t1Split[0].trim();

                    String[] t2Split = detail.split(ESC_SEPERATOR);
                    String t2BusinessId = t2Split[0].trim();
                    if (t1BusinessId.equals(t2BusinessId))
                    {
                        context.write(new Text(t1BusinessId), new Text(
                                t2Split[1] + "\t" + t2Split[2] + "\t" +
                                        t1Split[1]));
                        break;
                    }
                }
            }
        }
    }
    //Driver Program
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException
    {

        Configuration config1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config1, args).getRemainingArgs();
        //get All args
        if (otherArgs.length != 4)
        {
            System.err.println("Argument problem");
            System.err.println("AvTop10Business <review.csv> <business.csv> <temp_output> <final_output>");
            System.exit(0);
        }

         // create a job with name "Top10BusinessRatingJob"
        Job top10BusRatJob = Job.getInstance(config1, "Top10BusinessRating");
        top10BusRatJob.setJarByClass(AvTop10Business.class);
        top10BusRatJob.setMapperClass(Top10BusinessRatingMapper.class);
        top10BusRatJob.setReducerClass(Top10BusinessRatingReducer.class);
        // set output key type
        top10BusRatJob.setOutputKeyClass(Text.class);
        // set output value type
        top10BusRatJob.setOutputValueClass(Text.class);

        //set the HDFS path of the input data
        FileInputFormat.addInputPath(top10BusRatJob, new Path(otherArgs[REVIEW_FILE_POS]));
        // set the HDFS path for the temporary output file
        FileOutputFormat.setOutputPath(top10BusRatJob, new Path(otherArgs[TEMP_FILE_POS]));

        boolean isTop10BusRatJobComplete = top10BusRatJob.waitForCompletion(true);
        if (isTop10BusRatJobComplete){
            Configuration config2 = new Configuration();
            // create a job with name "Top10BusinessRatingJob"
            Job top10BusDetailsRatJob = Job.getInstance(config2, "Top10BusDetailsRatJob");

            top10BusDetailsRatJob.setJarByClass(AvTop10Business.class);
            System.out.println(args[TEMP_FILE_POS]);
            System.out.println(args[BUSINESS_FILE_POS]);
            System.out.println(args[FINAL_OUTPUT_POS]);
            MultipleInputs.addInputPath(top10BusDetailsRatJob, new Path(args[TEMP_FILE_POS]),
                    TextInputFormat.class, TopTenRatedBusinessRatingMapper.class);
            MultipleInputs.addInputPath(top10BusDetailsRatJob, new Path(args[BUSINESS_FILE_POS]),
                    TextInputFormat.class, AllBusinessDetailsMapper.class);

            // set output key type
            top10BusDetailsRatJob.setOutputKeyClass(Text.class);
            // set output value type
            top10BusDetailsRatJob.setOutputValueClass(Text.class);
            // set the HDFS path of the temp input data
            top10BusDetailsRatJob.setInputFormatClass(TextInputFormat.class);
            //set the HDFS path of the output data
            top10BusDetailsRatJob.setOutputFormatClass(TextOutputFormat.class);


            top10BusDetailsRatJob.setReducerClass(TopTenRatedBusinessDetailReducer.class);
            FileOutputFormat.setOutputPath(top10BusDetailsRatJob, new Path(args[FINAL_OUTPUT_POS]));

            System.exit(top10BusDetailsRatJob.waitForCompletion(true)? 0 : 1);
        }
    }
}