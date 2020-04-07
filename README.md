# Sequence Alignment Memorizer (SeAlM)

### Tested Dependencies
```$xslt
cmake >3.10
gcc >7.5.0 | clang >6.0.0
```

### Installation
```$xslt
git clone 
cmake CMakeLists.txt ---DCMAKE_BUILD_TYPE=Release
make
```

### Using Configuation File
```$xslt
./SeAlM --from_config <path_to_ini>
```

### Configuration File Parameters
##### I/O Parameters
```input_pattern``` regex for FASTQ or FASTA files used as input (reads)

```data_dir``` directory in which to search for input files

```reference``` path to indexed reference (type depends on aligner)

```output_ext``` by default, the output prefix is the same as input prefix 
with this added extension [e.g. .sam or .bam] (support varys by aligner)

##### Cache Parameters
```cache_policy``` cache eviction policy to use [none, lru, mru]

##### Query Block Parameters
```hash_func``` hash function used to group similar queries based on prefix length [none, single, double, triple]
