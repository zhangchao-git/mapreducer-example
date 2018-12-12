package cn.zoua.hadoop.mr.flowsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by abc .
 * <p>
 * 这个类就是mr程序运行时候的主类，本类中组装了一些程序运行时候所需要的信息
 * 比如：使用的是那个Mapper类  那个Reducer类  输入数据在那 输出数据在什么地方
 */
public class FlowDriver {

    public static void main(String[] args) throws Exception {
        //通过Job来封装本次mr的相关信息
        Configuration conf = new Configuration();
        // 即使没有下面这行,也可以本地运行 因\hadoop-mapreduce-client-core-2.7.4.jar!\mapred-default.xml 中默认的参数就是 local
        //conf.set("mapreduce.framework.name","local");
        Job job = Job.getInstance(conf);

        //指定本次mr job jar包运行主类
        job.setJarByClass(FlowDriver.class);

        //指定本次mr 所用的mapper reducer类分别是什么
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //指定本次mr mapper阶段的输出  k  v类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //指定本次mr 最终输出的 k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //上面的路径是本地测试时使用，如果要打包jar到hdfs上运行时，需要使用下面的路径。
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // job.submit(); //一般不要这个.
        //提交程序  并且监控打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}