#!/usr/bin/env python
desc="""Index and access files from TAR archive(s).
Note, it works only with raw (uncompressed) TAR archives!
But you can compress each file using zlib before adding it to TAR:) 
"""
epilog="""Author:
l.p.pryszcz@gmail.com

Mizerow, 23/04/2014
"""

import argparse, os, sys
import sqlite3, tarfile
from datetime import datetime

def get_cursor(indexfn):
    """Return cur object"""
    #create/connect to sqlite3 database
    cnx = sqlite3.connect(indexfn)
    cur = cnx.cursor()
    #asyn execute >50x faster ##http://www.sqlite.org/pragma.html#pragma_synchronous 
    #cur.execute("PRAGMA synchronous=OFF")
    #prepare db schema #type='table' and
    cur.execute("SELECT * FROM sqlite_master WHERE name='file_data'")
    result = cur.fetchone()
    if not result:
        #create file data table
        cur.execute("""CREATE TABLE file_data (file_id INTEGER PRIMARY KEY,
                    file_name TEXT, mtime FLOAT)""")
        cur.execute("CREATE INDEX file_data_file_name ON file_data (file_name)")
        #create offset_data table
        cur.execute("""CREATE TABLE offset_data (file_id INTEGER,
                    file_name TEXT, offset INTEGER, file_size INTEGER,
                    PRIMARY KEY (file_id, file_name))""")
        cur.execute("CREATE INDEX offset_data_file_name ON offset_data (file_name)")
    return cur

def prepare_db(cur, tarfn, verbose):
    """Prepare database and add file"""
    mtime = os.path.getmtime(tarfn)
    #check if file already indexed
    cur.execute("SELECT file_id, mtime FROM file_data WHERE file_name = ?", (tarfn,))
    result = cur.fetchone()#; print result
    if result:
        file_id, pmtime = result#; print file_id, pmtime
        #skip if index with newer mtime than archives exists
        if mtime <= pmtime:
            if verbose:
                sys.stderr.write(" Archive already indexed.\n")
            return
        #else update mtime and remove previous index
        cur.execute("UPDATE file_data SET mtime = ? WHERE file_name = ?", (mtime, tarfn))
        cur.execute("DELETE FROM offset_data WHERE file_id = ?", (file_id,))
    else:
        cur.execute("SELECT MAX(file_id) FROM file_data")
        max_file_id, = cur.fetchone()#; print max_file_id
        if max_file_id:
            file_id = max_file_id + 1
        else:
            file_id = 1
        #add file info
        cur.execute("INSERT INTO file_data VALUES (?, ?, ?)", (file_id, tarfn, mtime))
    return file_id

def index_tar(tarfn, indexfn, verbose):
    """Index tar file."""
    if verbose:
        sys.stderr.write("%s     \n"%tarfn)
    if not indexfn:
        indexfn = dbtarfile+".idx"
    #get archive size
    tarsize = os.path.getsize(tarfn)
    #prepare db
    cur = get_cursor(indexfn)
    file_id = prepare_db(cur, tarfn, verbose)
    if not file_id:
        return
    #index tar file #py2.6 compatible
    #with tarfile.open(tarfn) as tar:
    tar = tarfile.open(tarfn)
    for i, tarinfo in enumerate(tar, 1):
        cur.execute("INSERT INTO offset_data VALUES (?, ?, ?, ?)", \
                    (file_id, tarinfo.name, tarinfo.offset_data, tarinfo.size))
        # free ram...
        if not i%100:
            tar.members = []
            if verbose:
                sys.stderr.write(" %s [%.2f%s]      \r"%(i, tarinfo.offset_data*100.0/tarsize, '%'))
    #finally commit changes
    cur.connection.commit()

def tar_lookup(indexfn, file_name):
    """Yield files matching file_name from TAR archives."""
    cur = get_cursor(indexfn)
    cur.execute("""SELECT o.file_name, f.file_name, offset, file_size
                FROM offset_data as o JOIN file_data as f ON o.file_id=f.file_id
                WHERE o.file_name=?""", (file_name,))
    for fname, tarfn, offset, file_size in cur.fetchall():
        #open tarfile
        tarf = open(tarfn)
        #seek
        tarf.seek(offset)
        #yield file
        yield fname, tarfn, tarf.read(file_size)

def main():
    usage   = "%(prog)s [options]" 
    parser  = argparse.ArgumentParser(usage=usage, description=desc, epilog=epilog)
  
    parser.add_argument("-v", dest="verbose",   default=False, action="store_true", 
                        help="verbose")    
    parser.add_argument('--version', action='version', version='1.0')   
    parser.add_argument("-i", "--input", nargs="+", 
                        help="tar file(s)  [%(default)s]")
    parser.add_argument("-d", "--index", default=None,
                        help="index file  [input.idx]")
    parser.add_argument("-f", "--file",  default=None, nargs="+",
                        help="retrieve file(s) from archive")
    parser.add_argument("--cleanup", default=False, action="store_true",
                        help="remove archives from index that are not in input set")
                          
    o = parser.parse_args()
    if o.verbose:
        sys.stderr.write("Options: %s\n" % str(o))
        
    #retrieve file(s) from tar
    if o.file:
        if o.input:
            indexfn = o.input[0]+".idx"
        elif o.index:
            indexfn = o.index
        else:
            sys.stderr.write("Provide index path or tar file path first!\n")
            sys.exit(1)
        for i, fn in enumerate(o.file, 1):
            for j, (fname, tarfn, content) in enumerate(tar_lookup(indexfn, fn), 1):
                if o.verbose:
                    sys.stderr.write("%s %s %s %s\n"%(i, j, fname, tarfn))
                print content
    #index tar files            
    elif o.input:
        #first need to remove files not present in
        if o.cleanup:
            sys.stderr.write("This feature is not yet implemented!\n")
        for tarfn in o.input:
            #index tar
            index_tar(tarfn, o.index, o.verbose)
        
	
if __name__=='__main__': 
    t0  = datetime.now()
    try:
        main()
    except KeyboardInterrupt:
        sys.stderr.write("\nCtrl-C pressed!      \n")
    dt  = datetime.now()-t0
    sys.stdout.write( "#Time elapsed: %s\n" % dt )

    