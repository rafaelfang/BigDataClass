package edu.missouri.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<String> friendsSet=new HashSet<String>();
		String commonFriendsString="";
	
		for (Text v : values) {
			String[] arr=v.toString().split(" ");
			for(String aFriendString:arr){
				if(friendsSet.contains(aFriendString)){
					commonFriendsString+=aFriendString;
				}else{
					friendsSet.add(aFriendString);
				}
			}
			
		}
		if(!commonFriendsString.isEmpty()){
			context.write(key, new Text(commonFriendsString));
		}
	}
}