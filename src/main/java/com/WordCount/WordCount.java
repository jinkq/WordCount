package com.WordCount;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparable;


public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();//停词
    private Set<String> punctuations = new HashSet<String>();//标点符号

    private Configuration conf;
    private BufferedReader fis;

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      conf = context.getConfiguration();
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
      if (conf.getBoolean("wordcount.skip.patterns", false)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        
        URI patternsURI = patternsURIs[0];
        Path patternsPath = new Path(patternsURI.getPath());
        String patternsFileName = patternsPath.getName().toString();
        parseSkipFile(patternsFileName);

        URI punctuationsURI = patternsURIs[1];
        Path punctuationsPath = new Path(punctuationsURI.getPath());
        String punctuationsFileName = punctuationsPath.getName().toString();
        parsePunctuations(punctuationsFileName);

        // for (URI patternsURI : patternsURIs) {
        //   Path patternsPath = new Path(patternsURI.getPath());
        //   String patternsFileName = patternsPath.getName().toString();
        //   parseSkipFile(patternsFileName);
        // }
      }
      // parseSkipFile("skip/stop-word-list.txt");
      // parsePunctuations("skip/punctuations.txt");
    }

    private void parseSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + StringUtils.stringifyException(ioe));
      }
    }

    private void parsePunctuations(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String punctuation = null;
        while ((punctuation = fis.readLine()) != null) {
          punctuations.add(punctuation);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + StringUtils.stringifyException(ioe));
      }
    }

    private Boolean isNumeric(String str) {
      Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
      return pattern.matcher(str).matches();
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString().toLowerCase(); //全部转小写，不用命令行判断
      for (String pattern : punctuations) {
        line = line.replaceAll(pattern, " ");
      }
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {//单词
        String str = itr.nextToken();
        Boolean shouldRemove = false;

        //判断数字、长度
        if(isNumeric(str)||str.length()<3)
        {
          continue;
        }

        // 判断是不是停词，
        for(String pattern : patternsToSkip) {
          if(pattern.equals(str)) {
            shouldRemove = true;
            break;
          }
        }
        
        if (shouldRemove) {
          continue;
        }
        // word.set(itr.nextToken());
        word.set(str);
        context.write(word, one);
        Counter counter = context.getCounter(CountersEnum.class.getName(),
            CountersEnum.INPUT_WORDS.toString());
        counter.increment(1);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  private static class IntWritableDecreasingComparator extends IntWritable.Comparator {  
      public int compare(WritableComparable a, WritableComparable b) {  
        return -super.compare(a, b);  
      }  

      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {  
          return -super.compare(b1, s1, l1, b2, s2, l2);  
      }  
  }

  public static class SortReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
    private Text result = new Text();
    int rank=1;

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) 
     throws IOException, InterruptedException{
      for(Text val: values){
        if(rank > 100)
        {
          break;
        }
        result.set(val.toString());
        String str=rank+": "+result+", "+key;
        rank++;
        context.write(new Text(str),NullWritable.get());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    // if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
    //   System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
    //   System.exit(2);
    // }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));

    Path tempDir = new Path("tmp-" + Integer.toString(  
            new Random().nextInt(Integer.MAX_VALUE))); //第一个job的输出写入临时目录
    // FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
    FileOutputFormat.setOutputPath(job, tempDir);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    // System.exit(job.waitForCompletion(true) ? 0 : 1);
   

    if(job.waitForCompletion(true))  
    {  
        //新建一个job处理排序和输出格式
        Job sortJob = new Job(conf, "sort");  
        sortJob.setJarByClass(WordCount.class);  

        FileInputFormat.addInputPath(sortJob, tempDir); 

        sortJob.setInputFormatClass(SequenceFileInputFormat.class);  
        
        //map后交换key和value
        sortJob.setMapperClass(InverseMapper.class);  
        sortJob.setReducerClass(SortReducer.class);
        
        FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs.get(1)));  

        sortJob.setOutputKeyClass(IntWritable.class);  
        sortJob.setOutputValueClass(Text.class); 

        //排序改写成降序
        sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);  

        System.exit(sortJob.waitForCompletion(true) ? 0 : 1); 
    }  

    FileSystem.get(conf).deleteOnExit(tempDir);
  }
}
