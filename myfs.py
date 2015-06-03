#!/usr/bin/env python

import os
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import traceback
import errno

from fuse import FUSE, FuseOSError, Operations

import sqlite3

class DbFS(Operations):

    def __init__(self):
        #print os.path.abspath('.')
        self.DBFILE = './database/data.db'

        try: 
            conn = sqlite3.connect(self.DBFILE, timeout=600.0)
            c = conn.cursor()
            c.execute("""
                create table if not exists files (
                    path text primary key,
                    mode integer,
                    size integer,
                    ctime real,
                    mtime real,
                    atime real,
                    uid integer,
                    gid integer,
                    nlink integer
                )
            """)
            c.execute("""
                create table if not exists data (
                    path text primary key, 
                    content blob
                )
            """)
            now = time()
            c.execute("""
                insert or ignore into files
                (path,mode,size,ctime,mtime,atime,uid,gid,nlink)
                values (?,?,?,?,?,?,?,?,?)
            """, ('/', (S_IFDIR | 0755), 0, now, now, now, 0, 0, 2))
            conn.commit()
            c.close()
            conn.close()
        except:
            print 'aha some problems. '
            traceback.print_exc()
            pass
        self.fd = 0

    def chmod(self, path, mode):
        print 'chmod: %s' % path

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        c.execute("select mode from files where path=?",(path,))
        fetch = c.fetchone()
        if fetch is not None:
            mode_orig = fetch[0]
            mode_orig &= 0770000
            mode_orig |= mode
            print 'new mode: ', mode_orig
            c.execute("update files set mode=? where path=?", (mode_orig, path))
        else:
            raise IOError(errno.ENOENT,path)

        conn.commit()
        c.close()
        conn.close()

        return 0

    def chown(self, path, uid, gid):
        print 'chown: %s' % path

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        c.execute("update files set uid=? where path=?", (uid, path))
        c.execute("update files set gid=? where path=?", (gid, path))

        conn.commit()
        c.close()
        conn.close()

    def create(self, path, mode):
        print 'create: %s' % path

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        now = time()
        c.execute("""
            insert or ignore into files
            (path,mode,size,ctime,mtime,atime,uid,gid,nlink)
            values (?,?,?,?,?,?,?,?,?)
        """, (path, (S_IFREG | mode), 0, now, now, now, 0, 0, 1))

        conn.commit()
        c.close()
        conn.close()

        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        print 'getattr: %s' % path

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()
        c.execute("select * from files where path=?",(path,))
        fetch = c.fetchone()
        if fetch is not None:
            attr = dict(st_mode=fetch[1], st_size=fetch[2], st_ctime=fetch[3], st_mtime=fetch[4], st_atime=fetch[5], st_uid=fetch[6], st_gid=fetch[7], st_nlink=fetch[8])
        else:
            raise FuseOSError(errno.ENOENT)

        c.close()
        conn.close()

        print 'attr_size: ', attr['st_size']

        return attr

    def getxattr(self, path, name, position=0):
        print 'getxattr: %s' % path

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()
        c.execute("select * from files where path=?",(path,))
        fetch = c.fetchone()
        if fetch is not None:
            attr = dict(st_mode=fetch[1], st_size=fetch[2], st_ctime=fetch[3], st_mtime=fetch[4], st_atime=fetch[5], st_uid=fetch[6], st_gid=fetch[7], st_nlink=fetch[8])
        else:
            raise FuseOSError(errno.ENOENT)

        c.close()
        conn.close()

        try:
            return attr[name]
        except KeyError:
            return ''

    def listxattr(self, path):
        print 'listxattr: %s' % path
        return ['st_mode', 'st_size', 'st_ctime', 'st_mtime', 'st_atime', 'st_uid', 'st_gid', 'st_nlink']

    def mkdir(self, path, mode):
        print 'mkdir: %s' % path

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        now = time()
        c.execute("""
            insert or ignore into files
            (path,mode,size,ctime,mtime,atime,uid,gid,nlink)
            values (?,?,?,?,?,?,?,?,?)
        """, (path, (S_IFDIR | mode), 0, now, now, now, 0, 0, 2))

        c.execute("update files set nlink=nlink+1 where path='/'")

        conn.commit()
        c.close()
        conn.close()

    def open(self, path, flags):
        print 'open: %s' % path

        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        print 'read', path, size, offset, fh

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        c.execute("select content from data where path=?", (path,))
        fetch = c.fetchone()
        if fetch is not None:
            content = fetch[0]
        else:
            # raise IOError(errno.ENOENT,path)
            content = ''

        c.close()
        conn.close()

        return content[offset:offset + size]

    def readdir(self, path, fh):
        print 'readdir: %s' % path

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        c.execute("select path from files where path!='/'")
        fetch = c.fetchall()
        if fetch is not None:
            dirlist = ['.', '..'] + [x[0][1:] for x in fetch]
        else:
            dirlist = ['.', '..']

        c.close()
        conn.close()

        return dirlist

    def readlink(self, path):
        print 'readlink: %s' % path

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        c.execute("select content from data where path=?", (path,))
        fetch = c.fetchone()

        c.close()
        conn.close()

        return fetch[0]

    # TO DO removexattr

    def rename(self, old, new):
        print 'rename from %s to %s' %(old, new)

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        c.execute("update files set path=? where path=?", (new, old))

        conn.commit()
        c.close()
        conn.close()

    def rmdir(self, path):
        print 'rmdir: %s' % path

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        c.execute("delete from files where path=?", (path,))
        c.execute("update files set nlink=nlink-1 where path='/'")


        conn.commit()
        c.close()
        conn.close()

    # TO DO setxattr

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        print 'symlink src: %s -> tg: %s' %(source, target)

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        now = time()
        c.execute("""
            insert or ignore into files
            (path,mode,size,ctime,mtime,atime,uid,gid,nlink)
            values (?,?,?,?,?,?,?,?,?)
        """, (target, (S_IFLNK | 0777), len(source), now, now, now, 0, 0, 1))
        c.execute("""
            insert or ignore into data
            (path, content)
            values (?,?)
        """, (target, source))

        conn.commit()
        c.close()
        conn.close()

    def truncate(self, path, length, fh=None):
        print 'truncate: %s' % path

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        c.execute("select content from data where path=?", (path,))
        fetch = c.fetchone()
        if fetch is not None:
            content = fetch[0]
        else:
            # raise IOError(errno.ENOENT,path)
            content = ''

        content = content[:length]

        c.execute("update data set content=? where path=?", (content, path))
        c.execute("update files set size=? where path=?", (length, path))

        conn.commit()
        c.close()
        conn.close()

    def unlink(self, path):
        print 'unlink: ', path

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        c.execute("delete from files where path=?", (path,))

        conn.commit()
        c.close()
        conn.close()


    def utimens(self, path, times=None):
        print 'utimens', path

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        now = time()
        atime, mtime = times if times else (now, now)
        c.execute("update files set atime=? where path=?", (atime, path))
        c.execute("update files set mtime=? where path=?", (mtime, path))

        conn.commit()
        c.close()
        conn.close()


    def write(self, path, data, offset, fh):
        print 'write: %s' % path
        print 'data type: ', type(data)

        conn = sqlite3.connect(self.DBFILE, timeout=600.0)
        c = conn.cursor()

        c.execute("select content from data where path=?", (path,))
        fetch = c.fetchone()
        if fetch is not None:
            content = fetch[0]
        else:
            content = ''
        content = content[:offset] + data
        c.execute("""
                    insert or replace into data
                    (path,content)
                    values (?,?)
                """, (path, sqlite3.Binary(content)))
        c.execute("update files set size=? where path=?", (len(content), path))
        c.execute("select count(*) from data")
        number = (c.fetchone())[0]

        conn.commit()
        c.close()
        conn.close()

        return number

if __name__ == '__main__':
    if len(argv) != 2:
        print 'usage: %s <mntpt>' % (argv[0])
        exit(1)


    fuse = FUSE(DbFS(), argv[1], foreground=True)
