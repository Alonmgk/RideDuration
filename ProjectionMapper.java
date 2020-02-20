package com.rukbysoft.examples.regressionMR;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class ProjectionMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	  @Override
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		  //Should add code to check to handle issues with the data
		  String line = value.toString();
		  String[] ScoreData = line.split(",");
		  // Since we are trianing our model on seven columns
		  //1-->vendor_id,
		  //3-->passenger_count,
		  //4-->pickup_longitude,
		  //5-->pickup_latitude,
		  //6-->dropoff_longitude,
		  //8-->dropoff_latitude,
		  //4-->trip_duration
	      IntWritable userId = new IntWritable(Integer.parseInt(ScoreData[1]));
	      Text intervalscore = new Text(ScoreData[1].toString() + "-"+ScoreData[3].toString() + "-" +ScoreData[4].toString() + ScoreData[5].toString()+ "-"+ScoreData[6].toString()+ScoreData[7].toString()+"-"+ScoreData[8].toString());
	      context.write(userId, intervalscore);
	  }

	}