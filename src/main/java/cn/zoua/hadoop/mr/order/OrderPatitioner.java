package cn.zoua.hadoop.mr.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPatitioner extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        //按照key的orderid进行分区
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % i;
    }
}
