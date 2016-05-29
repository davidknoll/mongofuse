# mongofuse
A FUSE filesystem, backed by MongoDB and written in Node.js. Mostly an exercise for me to learn MongoDB and Node.js.

### Starting
If cloned from GitHub, you'll need to run `npm install` from the directory of
your clone, then any of:
* `npm start connection-string mountpoint`
* `node index.js connection-string mountpoint`
* `./index.js connection-string mountpoint`

You can also install globally with `npm install -g mongofuse`, in which case run:
* `mongofuse connection-string mountpoint`

where `connection-string` is a MongoDB [connection string](https://docs.mongodb.com/manual/reference/connection-string/)
(maybe just `mongofuse` to use a database called `mongofuse` on an instance
running on localhost on the standard port) and `mountpoint` is an existing
directory where the filesystem is to be mounted.

### Things that work
* The root directory is now created automatically when you start mongofuse with
an empty database. This means you can actually try using it without manually
inserting stuff into MongoDB first!
* The usual reading, writing, creating, deleting, renaming etc. of files and directories
* chmod, chown, chgrp (including updating ctime)
* mknod (special files can be created, but can't be used with nodev in effect, see below)
* mtime/ctime update on file write
* symlinks

### Things that don't work / aren't present (yet)
* Permissions aren't enforced. You can write to a file with mode 000.
You can't now, however, chmod/chown/chgrp when you shouldn't be allowed.
* atimes aren't updated automatically. Nor are directory mtimes/ctimes when a file is created.
* No way of specifying mount options on the command line (seems to default to nosuid, nodev)
* hardlinks
* extended attributes
* Files larger than just under 16MB, due to the maximum document size in MongoDB.
I now check for this and return EFBIG from ftruncate/truncate/write.
The solution to this is [GridFS](https://docs.mongodb.com/manual/core/gridfs/).
* I don't know if it works on OSes other than Linux, I haven't tried.
