## tar_indexer repository

TAR indexer uses sqlite3 for index storing and allows indexing of multiple tar archives. 
Note, only raw (uncompressed) tar files are accepted as native tar.gz cannot be random accessed. 
But you can compress each file using zlib before adding it to tar. 

```bash
# index content of multiple tar archives
tar2index.py -v -i db_*/*.tar -d archives.db3

# search for some_file in mutliple arhcives
tar2index.py -v -f some_file -d archives.db3
```
