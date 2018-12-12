package cn.zoua.hadoop.mr.flowsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    FlowBean flowBean = new FlowBean();
    Text v = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 将一行内容转成string
        String ling = value.toString();

        // 2 切分字段
        String[] fields = ling.split("\t");

        // 3 封装对象及获取电话号码
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        v.set(fields[0]);
        flowBean = new FlowBean(upFlow, downFlow);


        // 4 写出数据
        context.write(flowBean, v);

    }
}
