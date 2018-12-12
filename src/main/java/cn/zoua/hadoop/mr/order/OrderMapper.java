package cn.zoua.hadoop.mr.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    OrderBean bean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        // 1 获取一行数据
        String line = value.toString();

        // 2 截取字段
        String[] fields = line.split("\t");
        System.out.println();
        for (int i = 0; i < fields.length; i++) {
            System.out.print( fields[i]+"||");
        }
        // 3 封装bean
        bean.setOrderId(fields[0]);
        bean.setPrice(Double.parseDouble(fields[2]));

        // 4 写出
        context.write(bean, NullWritable.get());
    }
}
