package cn.zoua.hadoop.mr.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    public void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFlow = 0;

        // 1 遍历所用bean，将其中的上行流量，下行流量分别累加
        for (FlowBean bean : values) {
            sum_upFlow += bean.getUpFlow();
            sum_downFlow += bean.getDownFlow();
        }

        // 2 封装对象
        FlowBean resultBean = new FlowBean(sum_upFlow, sum_downFlow);
        context.write(key, resultBean);

    }

}
