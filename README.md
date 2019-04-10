# neatlib

*Huang Jiahua*

## Features
1. Fast and safe sequential hash table basic_hash_table.
2. Fast and safe wait-free concurrent hash table concurrent_hash_table. 

## TODO
1. Add lock-free support to concurrent hash table.
2. Achieve better memory reclamation policy (currently using reference counting). (partly done)


## Building
- Currently the project don't need to be built, just include the the header file is enough.
- To build the test, use cmake:

```bash
mkdir build && cd build
cmake ..
```

## Requirements
- Boost smart pointer library.