import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Top10MutualFriend {


    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{


        private Text mapKey = new Text(); // type of output key
        private Text value2 = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // split on '/n' gives all user in an array.
            String[]  allUsersFriendsArrays = value.toString().split("\n");
            for(String userFriendsDetailsText:allUsersFriendsArrays){

                String[] userFriendsDetailArray = userFriendsDetailsText.split("\t");
                //check if they have friends
                if(userFriendsDetailArray.length>1) {
                    String user = userFriendsDetailArray[0];
                    String userFriendsText = userFriendsDetailArray[1];


                    String[] userFriendsArray = userFriendsText.split(",");
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

    public static class Reduce
            extends Reducer<Text, Text, Text, IntWritable> {

        private java.util.Map<String, Integer> friendsCountMap = new HashMap<>();

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

            // step 1: Extract the friends.
            String[] bothFriendsArray = new String[2];

            int valueIndex = 0;
            int count = 0;
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
                    count = 0;
                    for (int i = 0; i < user1Friends.length; i++) {
                        for (int j = 0; j < user2Friends.length; j++) {
                            String friendI = user1Friends[i];
                            if (friendI.equals(user2Friends[j])) {
                                mutualFriendsList.add(friendI);
                                count++;
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
                    //System.out.println("List of Friend: - "+"key:"+ key+" - count :"+count +"\t" + mutualFriendString);
                    // add the values to Map.
                    friendsCountMap.put(key.toString(),count);
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Sort the Map based on values.
            Set<java.util.Map.Entry<String, Integer>> MutualFriendsSet = this.friendsCountMap.entrySet();
            List<java.util.Map.Entry<String, Integer>> MutualFriendsList = new ArrayList(MutualFriendsSet);

            //Descending order.
            Collections.sort(MutualFriendsList, new Comparator<java.util.Map.Entry<String, Integer>>() {
                @Override
                public int compare(java.util.Map.Entry<String, Integer> o1, java.util.Map.Entry<String, Integer> o2) {
                    return (o2.getValue().compareTo(o1.getValue()));
                }
            });

            int counter = 0;
            for (java.util.Map.Entry<String, Integer> entry : MutualFriendsList) {
                System.out.println(entry.getKey());
                if (counter < 10) {

                    context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
                    counter++;
                }
                else{
                    break;
                }
            }
        }


    }



    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length !=2) {
            System.err.println("Usage: TOP10MutualFriend <in> <out> ");
            //TODO: add friendlist to it.
            System.exit(2);

        }


        // create a job with name "Mutual Friend"
        Job job = new Job(conf, "Mutual Friend");
        job.setJarByClass(MutualFriend.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);


        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(IntWritable.class);

        //MAP Output.
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

