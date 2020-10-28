# WordCount

181098118 金可乔

[toc]
## 设计思路
在hadoop官方示例WordCount v2.0的基础上做修改，修改包括以下几个方面：
### 特殊词的忽略
#### 忽略标点符号

1. 设置了一个set名为`punctuations`存储从punctuation.txt里读出的标点符号

	```java
	private Set <String> punctuations = new HashSet<String>();
	```

2. 仿照`parseSkipFile`方法写了`parsePunctuations`方法
3. 在map方法里使用`replaceAll`方法，将标点符号替换为空格。若替换为空字符，会导致诸如Shakespeare's变成Shakespeares的错误

#### 忽略停词

1.  用名为`patternsToSkip`的set存储stop-word-list.txt里读出的停词
2. 沿用原有的`parseSkipFile`方法
3. 在map方法里对单词遍历的循环里，增加判断该词是否是停词的循环判断，若该词是停词，则外循环continue

#### 忽略数字和长度小于3的单词

1. 写一个正则匹配的`isNumeric()`方法，判断词语是不是数字
2. 在map方法里对单词遍历的循环里，调用`isNumeric`方法判断词语是不是数字，或者长度是不是小于3，若是，则continue

### 排序

1. 新增了一个名为sortJob的job来进行排序和修改输出格式的操作
2. 将原wordcount job的输出存储在一个临时文件夹，在sortJob中以该临时文件夹为input path
3. 调用hadoop内置的InverseMapper，map后交换key和value，即新的key为词语出现的频数，value为词语
4. 重写一个IntWritableDecreasingComparator类，按照新的key（频数）进行降序排序（原本默认是升序）

### 修改输出格式

1. 重写sortJob的reduce方法，设置计数变量，仅仅write前100个key-value对
2. 将新的key设置为一个符合输出格式的Text类（字符串拼接），value设为NullWritable，作为最终的输出形式

## 实验结果
出现频数排名前100的单词如下：
1: scene, 10241
2: thou, 9438
3: thy, 6592
4: shall, 6398
5: king, 6254
6: lord, 5702
7: sir, 5530
8: thee, 5381
9: good, 5123
10: come, 4473
11: enter, 4252
12: act, 4109
13: let, 4084
14: love, 3596
15: man, 3565
16: hath, 3379
17: like, 3346
18: henry, 3304
19: say, 3057
20: know, 3028
21: make, 2891
22: did, 2844
23: shakespeare, 2664
24: homepage, 2652
25: previous, 2458
26: tis, 2406
27: duke, 2260
28: speak, 2063
29: tell, 1918
30: father, 1909
31: think, 1890
32: exeunt, 1864
33: time, 1856
34: queen, 1836
35: life, 1773
36: heart, 1759
37: god, 1740
38: lady, 1736
39: death, 1695
40: exit, 1694
41: day, 1675
42: men, 1673
43: master, 1638
44: great, 1634
45: look, 1562
46: hear, 1558
47: doth, 1544
48: night, 1543
49: art, 1516
50: mistress, 1515
51: away, 1493
52: prince, 1472
53: hand, 1462
54: true, 1402
55: pray, 1373
56: sweet, 1360
57: richard, 1344
58: fair, 1340
59: gloucester, 1284
60: honour, 1255
61: house, 1226
62: falstaff, 1192
63: york, 1192
64: fear, 1187
65: leave, 1185
66: second, 1184
67: son, 1180
68: blood, 1168
69: world, 1160
70: old, 1160
71: antony, 1158
72: brother, 1138
73: heaven, 1123
74: eyes, 1117
75: john, 1104
76: grace, 1090
77: comes, 1089
78: till, 1082
79: poor, 1072
80: nay, 1061
81: noble, 1054
82: better, 1053
83: way, 1023
84: gentleman, 1021
85: myself, 1017
86: hast, 1007
87: stand, 990
88: bear, 978
89: page, 966
90: peace, 963
91: caesar, 963
92: head, 947
93: madam, 934
94: dead, 913
95: word, 911
96: mark, 911
97: wife, 893
98: live, 883
99: little, 883
100: place, 855

### web截图

![](https://finclaw.oss-cn-shenzhen.aliyuncs.com/img/pic3.png)

* wordcount job：

  ![](https://finclaw.oss-cn-shenzhen.aliyuncs.com/img/pic2.png)

* sort job:

  ![](https://finclaw.oss-cn-shenzhen.aliyuncs.com/img/pic1.png)

## 作业中遇到的问题

1. 在`localhost:8088`点击某个job的history显示unable to connect

   * 原因：未开启history server
   * 解决方法：在HADOOP_HOME目录下执行`sbin/mr-jobhistory-daemon.sh start historyserver`

2. 创建maven项目时卡在Generating project in Interactive mode

   ![](https://finclaw.oss-cn-shenzhen.aliyuncs.com/img/pic4.png)

   * 解决方法：在创建maven项目的指令后加参数`-DarchetypeCatalog=internal`，让它不要从远程服务器上取catalog

## 任务可以改进的地方

1. 可以进行名词单、复数的还原
2. 可以进行动词时态的还原

