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
time ./hw/t.txt 10 10

word:fdaffegdafafdafafeafgadfgdafagvfklgjkrljgkf seq:320000001

real	15m50.456s
user	3m50.579s
sys	13m4.622s
```
