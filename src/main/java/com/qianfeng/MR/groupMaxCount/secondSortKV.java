package com.qianfeng.MR.groupMaxCount;


        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.NullWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.util.Tool;
        import org.apache.hadoop.util.ToolRunner;
        import java.io.IOException;

public class secondSortKV extends ToolRunner implements Tool{
    static class MyMapper extends Mapper<LongWritable,Text,secondSortBean,Text>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        private static Text v = new Text("1");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] words = line.split(" ");

            int first = Integer.parseInt(words[0]);
            int second = Integer.parseInt(words[1]);
            secondSortBean k = new secondSortBean(first, second);

            context.write(k,v);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    private static LongWritable v = new LongWritable();

    static class MyReducer extends Reducer<secondSortBean,Text,secondSortBean,NullWritable>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(secondSortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        setConf(conf);

        //2、创建Job
        Job job = Job.getInstance(conf,"secondSortKV");

        //3、设置Job的执行路径
        job.setJarByClass(secondSortKV.class);

        //4、设置map端的属性
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(secondSortBean.class);
        job.setMapOutputValueClass(Text.class);

        //5、设置reduce端的属性
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(secondSortBean.class);
        job.setOutputValueClass(NullWritable.class);

        //6、设置输入输出的参数
        FileInputFormat.setInputPaths(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/secondSort"));

        FileOutputFormat.setOutputPath(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/secondKVSort02"));

        //7、提交job
        boolean b = job.waitForCompletion(true);

        return (b?0:1);
    }

    @Override
    public void setConf(Configuration conf) {
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");
    }

    @Override
    public Configuration getConf() {
        return new Configuration();
    }

    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(new Configuration(),new secondSortKV(),args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}