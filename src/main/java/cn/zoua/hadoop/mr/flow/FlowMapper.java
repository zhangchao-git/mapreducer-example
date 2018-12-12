package cn.zoua.hadoop.mr.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 将一行内容转成string
        String ling = value.toString();

        // 2 切分字段
        String[] fields = ling.split("\t");

        // 3 取出手机号码
        String phoneNum = fields[1];

        // 4 取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        // 5 写出数据
        context.write(new Text(phoneNum), new FlowBean(upFlow, downFlow));

    }
}
