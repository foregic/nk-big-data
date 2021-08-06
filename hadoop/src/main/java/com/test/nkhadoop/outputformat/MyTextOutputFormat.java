package com.test.nkhadoop.outputformat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MyTextOutputFormat extends TextOutputFormat<Text, Writable>{

}
