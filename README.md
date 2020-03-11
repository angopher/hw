
注：<font color='red'> 该分支是优化的版本，原始版本在master分支上 </font>

## 题目：有一个 100GB 的文件，里面内容是文本, 每行一个单词，换行符即单词分隔符，要求：
1. 找出第一个不重复的词
2. 只允许扫一遍原文件
3. 尽量少的 IO
4. 内存限制 16G
要求：用 C++ 完成，用尽可能高的效率实现

### 思路
三阶段
1. 将原始文件split到多个中间文件，每个单词带上序列号
2. 并行处理中间文件，算出每个中间文件的第一个uniq word
3. 合并结果

细节见代码和注释


## 编译

#### 编译器需求
g++-8

#### 编译命令：
g++-8 -O3 -o a main.cpp -std=c++17 -lstdc++fs

## 运行
./a /tmp/words.txt 16 10

#### 参数：
1. 输入文件，每行一个单词
2. 并行数，对标cpu核数
3. 文件需要切割数量

#### 输出：
word seq

#### example
```
lilideMacBook-Pro:hw lili$ time ./a ./t.txt 10 10
word:fdaffegdafafdafafeafgadfgdafagvfklgjkrljgkf seq:320000001

real	0m26.820s
user	1m16.134s
sys	0m11.923s
```

`跑了三千万行数据，对比master分支的原始版本有30多倍的效率提升`
