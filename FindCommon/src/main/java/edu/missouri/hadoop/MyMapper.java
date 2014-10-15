package edu.missouri.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String inputLine = value.toString();
		String[] personFriendsArray = inputLine.split(":");
		String personString = personFriendsArray[0];
		String[] friendsArray = personFriendsArray[1].split(" ");
		String outkey;
		
		for (String aFriendString : friendsArray) {
			if (personString.compareTo(aFriendString) < 0) {
				outkey = personString + aFriendString;
			} else {
				outkey = aFriendString + personString;
			}

			context.write(new Text(outkey), new Text(personFriendsArray[1]));
		}

	}
}
