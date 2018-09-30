package Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created with IDEA
 * author 光明顶斗士
 * Date:18-9-29
 * Time:下午4:51
 * Vision:1.1
 * （2）给出未来10天证书将要到期的农产品？以当前时间为准。10分
 */
public class problem2 {
    static class P2Maper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lines = value.toString();
            String[] strings = lines.split("\t");

            context.write(new Text(strings[5]),new Text(strings[4]));
        }
    }

    static class P2Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            TreeSet te = new TreeSet();
            while (iterator.hasNext()){
                te.add(iterator.next().toString()); //去重
            }
//            String s = new String();
            StringBuffer stringBuffer = new StringBuffer();
            Iterator iterator1 = te.iterator();
            while (iterator1.hasNext()){
                stringBuffer.append("\t"+iterator1.next().toString());
            }

            context.write(key,new Text(String.valueOf(stringBuffer)));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(configuration, "problem2");

        //5、设置Job的执行路径
        job.setJarByClass(problem2.class);

        //6、设置mapTask调用的业务逻辑类
        job.setMapperClass(P2Maper.class);

        //7、设置map端数据输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //8、设置mapTask调用的业务类
        job.setReducerClass(P2Reducer.class);

        //9、设置reduce端的数据的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //10、设置Job的输入文件的路径
        FileInputFormat.setInputPaths(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/team"));

        //11、设置Job的输出文件的路径
        FileOutputFormat.setOutputPath(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/teamout"));

        Boolean b  = job.waitForCompletion(true);
        System.exit(b ? 0:1);
    }
}
