package Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created with IDEA
 * author 光明顶斗士
 * Date:18-9-29
 * Time:下午5:06
 * Vision:1.1
 */
public class problem4 {
    static class P4Maper extends Mapper<LongWritable, Text, Text, Text> {
        private  static ArrayList<String> arrayList = new ArrayList<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            BufferedReader bf = null;
            Path[] localCacheFiles = context.getLocalCacheFiles();
            Path  path = localCacheFiles[0];
            System.out.println(path.getName());

            bf = new BufferedReader(new FileReader(path.toUri().getPath()));
                    String str = null;
                    while((str = bf.readLine()) != null){
                        String[] minggan1 = str.split(" ");
                        Collections.addAll(arrayList, minggan1);

                    }
            bf.close();
            }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lines = value.toString();
            String[] strings = lines.split(" ");
            StringBuffer stringBuffer = new StringBuffer();
            for (String s : strings){
                if (!arrayList.contains(s)){
                    stringBuffer.append(" "+s);
                }
            }
            context.write(new Text(String.valueOf(stringBuffer)),new Text(""));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(configuration, "problem4");

        //5、设置Job的执行路径
        job.setJarByClass(problem4.class);

        //6、设置mapTask调用的业务逻辑类
        job.setMapperClass(problem4.P4Maper.class);

        //7、设置map端数据输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        job.addCacheFile(new URI("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/sens/sensitive.txt"));
        //10、设置Job的输入文件的路径
        FileInputFormat.setInputPaths(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/team1"));
        //11、设置Job的输出文件的路径
        FileOutputFormat.setOutputPath(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/teamout3"));

        Boolean b  = job.waitForCompletion(true);
        System.exit(b ? 0:1);
    }
}
