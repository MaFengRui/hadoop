package com.qianfeng.MR.RelationJoin;//package com.qianfeng.MR.RelationJoin;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Iterator;
//
//    /**
//     * Created with IDEA
//     * author 光明顶斗士
//     * Date:18-9-27
//     * Time:下午9:07
//     * Vision:1.1
//     *
//     * 思路：
//     * Mapper 儿子　爹　
//     * reducer 爹　　爷
//     */
//    public class child_grandParent extends ToolRunner implements Tool {
//        @Override
//        public int run(String[] strings) throws Exception {
//            Configuration configuration = getConf();
//            setConf(configuration);
//
//            Job job = Job.getInstance(configuration,"child_grandParent");
//
//            //map
//            job.setMapOutputKeyClass(child_grandParentMapper.class);
//            job.setMapOutputKeyClass(LongWritable.class);
//            job.setMapOutputValueClass(Text.class);
//
//            //reduce
//            job.setReducerClass(child_grandParentReducer.class);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(Text.class);
//
//            //输入输出路径
//            FileInputFormat.setInputPaths(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/relationData"));
//            FileOutputFormat.setOutputPath(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/relationOutput"));
//
//            //提交
//            boolean b = job.waitForCompletion(true);
//            return b?0:1;
//
//
//        }
//
//        @Override
//        public void setConf(Configuration configuration) {
//            configuration.set("fs.defaultFS","file:///");
//            configuration.set("mapreduce.framework.name","local");
//        }
//
//        @Override
//        public Configuration getConf() {
//            return new Configuration();
//        }
//
//        static class child_grandParentMapper extends Mapper<LongWritable, Text, Text, Text> {
//            @Override
//            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//
//                String[] strings = value.toString().split(" ");
//
//                context.write(new Text(strings[1]), new Text("1-" + strings[0]));
//
//                context.write(new Text(strings[0]), new Text("2-" + strings[1]));
//
//            }
//        }
//
//        static class child_grandParentReducer extends Reducer<Text, Text, Text, Text> {
//            @Override
//            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//                Iterator iterator = values.iterator();
//                ArrayList listchild = new ArrayList();
//                ArrayList listParent = new ArrayList();
//
//                while (iterator.hasNext()) {
//                    String strings = iterator.next().toString();
//                    if (strings.charAt(0) == '1') {
//                        listchild.add(strings.split("-")[1]);
//                    } else if (strings.charAt(0) == '1') {
//                        listParent.add(strings.split("-")[1]);
//                    }
//                }
//                for (int i = 0; i < listchild.size(); i++) {
//                    for (int j = 0; j < listParent.size(); j++) {
//                        context.write(new Text(listchild.get(i).toString()), new Text(listchild.get(j).toString()));
//                    }
//                }
//            }
//
//        }
//
//        public static void main(String[] args) throws Exception {
//            System.exit(ToolRunner.run(new Configuration(),new child_grandParent(),args));
//
//        }
//    }

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.util.Tool;
        import org.apache.hadoop.util.ToolRunner;

        import java.io.IOException;
        import java.util.ArrayList;
        import java.util.Iterator;

/**
 * @ClassName model02
 * @Description TODO
 * @Author Chenfg
 * @Date 2018/9/26 0026 9:55
 * @Version 1.0
 */
public class child_grandParent extends ToolRunner implements Tool{
    static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        private static Text k = new Text();
        private static Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //
            String line = value.toString();

            String[] relations = line.split(" ");

            //1、写左表 p r-c parent
            k.set(relations[1]);
            v.set("1-"+relations[0]);
            context.write(k,v);

            //2、写右表 c r-p child
            k.set(relations[0]);
            v.set("2-"+relations[1]);
            context.write(k,v);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        private static Text k = new Text();
        private static Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //定义一个集合存放孙辈
            //定义一个集合存放爷爷辈
            ArrayList<String> grandchild = new ArrayList<String>();
            ArrayList<String> grandparent = new ArrayList<String>();

            Iterator<Text> it = values.iterator();

            while (it.hasNext()){
                String relation = it.next().toString();

                //判断关系
                if(relation.charAt(0) == '1'){
                    grandchild.add(relation.split("-")[1]);
                }else if(relation.charAt(0) == '2'){
                    grandparent.add(relation.split("-")[1]);
                }
            }

            //使用笛卡尔积关联查询
            for (int i = 0; i < grandchild.size(); i++) {
                for (int j = 0; j < grandparent.size() ; j++) {
                    k.set(grandchild.get(i));
                    v.set(grandparent.get(j));
                    context.write(k,v);
                }
            }
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
        Job job = Job.getInstance(conf,"singleTableJoin");

        //3、设置Job的执行路径
        job.setJarByClass(child_grandParent.class);

        //4、设置map端的属性
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //5、设置reduce端的属性
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //6、设置输入输出的参数
        FileInputFormat.setInputPaths(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/relationData"));

        FileOutputFormat.setOutputPath(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/relationOutput"));

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
            System.exit(ToolRunner.run(new Configuration(),new child_grandParent(),args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
