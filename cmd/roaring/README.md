Quick golang program to test storing 10M random integers up to 10-digits in length in a Roaring bitmap.

The golang program uses 3 different roaring bitmaps so that it can store all possible unsigned 10-digit integers.

Roaring bitmaps are used to store uint32 integers.  The max uint32 integer is 4294967295.  
So, to store 10-digit integers, we need to use 3 bitmaps.

Here we generate 10M random integers that all fit within a uint32 integer, and store them in a roaring bitmap.

It shows the total memory usage in the 3 bitmaps and that all 10M values are in the first bitmap.

```shell
seq 15000000 | awk 'BEGIN { srand(); scale=2^31 }  { print int(rand() * scale) }' | sort -u | shuf -n 10000000 | ./roaring
Memory size of bitmap 0: 20065544 bytes, 19.14MB
Cardinality of bitmap 0: 10000000
Memory size of bitmap 1: 8 bytes, 0.00MB
Cardinality of bitmap 1: 0
Memory size of bitmap 2: 8 bytes, 0.00MB
Cardinality of bitmap 2: 0
Total memory size: 20065560 bytes, 19.14MB
Cardinality: 10000000
```


Here, we generate 10M random integers using the full 10-digit range, these are stored across the 3 roaring bitmaps.

```shell
seq 15000000 | awk 'BEGIN { srand(); scale=10^10 }  { print int(rand() * scale) }' | sort -u | shuf -n 10000000 | ./roaring
Memory size of bitmap 0: 8723304 bytes, 8.32MB
Cardinality of bitmap 0: 4296112
Memory size of bitmap 1: 8715984 bytes, 8.31MB
Cardinality of bitmap 1: 4292452
Memory size of bitmap 2: 2865912 bytes, 2.73MB
Cardinality of bitmap 2: 1411436
Total memory size: 20305200 bytes, 19.36MB
Cardinality: 10000000
```

This shows that the memory usage is still quite low.  The roaring bitmaps are very efficient at storing large sets of integers.
