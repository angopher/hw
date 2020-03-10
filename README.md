## 题目：有一个 100GB 的文件，里面内容是文本, 每行一个单词，换行符即单词分隔符，要求：
1. 找出第一个不重复的词
2. 只允许扫一遍原文件
3. 尽量少的 IO
4. 内存限制 16G
要求：用 C++ 完成，用尽可能高的效率实现


## 编译
g++-8 -o a main.cpp -std=c++17 -lstdc++fs

## 运行
./a /tmp/test.txt 16 10

stdout => word:xjsbdfyvftkfuyx seq:3

