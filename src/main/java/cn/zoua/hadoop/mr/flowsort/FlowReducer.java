package cn.zoua.hadoop.mr.flowsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    public void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text t = values.iterator().next();
        context.write(t, key);

    }

}
