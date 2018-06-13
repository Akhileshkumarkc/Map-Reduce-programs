import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriend {

    public static final String FRIEND1 ="Friend1";
    public static final String FRIEND2 ="Friend2";

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{


        private Text mapKey = new Text(); // type of output key
        private Text value2 = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //get two users Input.

            Configuration config = context.getConfiguration();
            String friend1Str = config.get(FRIEND1);
            String friend2Str = config.get(FRIEND2);

            // split on '/n' gives all user in an array.
            String[]  allUsersFriendsArrays = value.toString().split("\n");
            for(String userFriendsDetailsText:allUsersFriendsArrays){

                String[] userFriendsDetailArray = userFriendsDetailsText.split("\t");
                //check if they have friends
                if(userFriendsDetailArray.length>1) {
                    String user = userFriendsDetailArray[0];
                    String userFriendsText = userFriendsDetailArray[1];


                    String[] userFriendsArray = userFriendsText.split(",");
                    if (user.equals(friend1Str) || user.equals(friend2Str)) {
                        for (String friend : userFriendsArray) {
                            //Order so that common key value pairs can combine.
                            String orderedFriend = user.compareTo(friend) < 0 ? user + "," + friend : friend + "," + user;
                            mapKey.set(orderedFriend);
                            value2.set(userFriendsText);
                            context.write(mapKey, value2);

                        }
                    }
                }
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

           // step 1: Extract the friends.
            String[] bothFriendsArray = new String[2];

            int valueIndex = 0;
            for(Text friendsCsv:value){
                bothFriendsArray[valueIndex] = friendsCsv.toString();
               // System.out.println("value ("+valueIndex+"): "+friendsCsv);
                valueIndex++;
            }

            ArrayList<String> mutualFriendsList = new ArrayList<>();

            // If Value Index has two list if they have Mutual Friends
            if(valueIndex >= 2) {
                String[] user1Friends = bothFriendsArray[0].split(",");
                String[] user2Friends = bothFriendsArray[1].split(",");


                //find Common Friends.
                if(user1Friends!= null && user2Friends!=null) {

                    for (int i = 0; i < user1Friends.length; i++) {
                        for (int j = 0; j < user2Friends.length; j++) {
                            String friendI = user1Friends[i];
                            if (friendI.equals(user2Friends[j])) {
                                mutualFriendsList.add(friendI);
                            }
                        }
                    }
                    //prepare MutualFriend Text.
                    StringBuffer mutualFriendText = new StringBuffer();
                    String mutualFriendString;

                    for (String mutualFriend : mutualFriendsList) {
                        mutualFriendText.append(mutualFriend).append(",");

                    }

                    //Handle last comma.
                    if (mutualFriendText.length() > 1) {
                        mutualFriendString = mutualFriendText.substring(0, mutualFriendText.length() - 1);//remove ,
                    } else {
                        mutualFriendString = "";
                    }

                    //print the results.
                    System.out.println("List of Friend: "+"key"+ "\t" + mutualFriendString);
                    result.set(mutualFriendString);
                    context.write(key, result); // create a pair <keyword, number of occurences>
                    }
                }
            }
        }



    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length !=4) {
            System.err.println("Usage: MutualFriend <in> <out> <friendID1> <friendID2>");
            //TODO: add friendlist to it.
            System.exit(2);

        }


        conf.set(FRIEND1,otherArgs[2]);
        conf.set(FRIEND2,otherArgs[3]);

        // create a job with name "Mutual Friend"
        Job job = new Job(conf, "Mutual Friend");
        job.setJarByClass(MutualFriend.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);


        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

