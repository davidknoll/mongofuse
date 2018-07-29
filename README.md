# mongofuse
A FUSE filesystem, powered by MongoDB and written in Node.js.
Mostly an exercise for me to learn MongoDB and Node.js.
It aims towards POSIX functionality for use as a regular filesystem.

### Starting
If cloned from GitHub, you'll need to run `npm install` from the directory of
your clone, then any of:
* `npm start connection-string mountpoint [-o option[,option]...]`
* `node index.js connection-string mountpoint [-o option[,option]...]`
* `./index.js connection-string mountpoint [-o option[,option]...]`

You can also install globally with `npm install -g mongofuse`, in which case run:
* `mongofuse connection-string mountpoint [-o option[,option]...]`

where `connection-string` is a MongoDB [connection string](https://docs.mongodb.com/manual/reference/connection-string/)
(maybe just `mongofuse` to use a database called `mongofuse` on an instance
running on localhost on the standard port) and `mountpoint` is an existing
directory where the filesystem is to be mounted.
Mount options can be [some of these](http://blog.woralelandia.com/2012/07/16/fuse-mount-options/),
as also linked from the [fuse-bindings](https://www.npmjs.com/package/fuse-bindings#mount-options) readme.

### Things that work
* The root directory is now created automatically when you start mongofuse with
an empty database. This means you can actually try using it without manually
inserting stuff into MongoDB first!
* The usual reading, writing, creating, deleting, renaming etc. of files and directories
* chmod, chown, chgrp (including updating ctime)
* mknod (special files can be created, but can't be used with nodev in effect, see below)
* mtime/ctime update on file write
* atime update on file read, directory list, symlink resolution. atime/relatime/noatime options.
* symlinks and hardlinks (won't hardlink directories)
* File permissions enforced on open, truncate, ftruncate.
Directory permissions enforced on opendir.
access function is implemented.
You can't read from a fd opened with O_WRONLY and vice versa.
You can't chmod/chown/chgrp when you shouldn't be allowed to.
* Accepts basic FUSE mount options on the command line (eg. `-o allow_other`)
* The size is now stored explicitly in the inode, avoiding looking up
the data if it's only the attributes we're interested in.
* Extended attributes

### Things that don't work / aren't present (yet)
* Directory permissions are partially but not fully enforced.
You can traverse and create/delete files in any directory,
although reading/listing permissions are enforced.
(Mounting with the `default_permissions` option is a possible solution.)
* Directory mtimes/ctimes aren't updated when a file is created.
* When I repeatedly `touch` a file without specifying a time, the atime/mtime
that are passed to my utimens function jump about non-monotonically over a
number of minutes. They can also be a number of minutes away from `Date.now()`
which is what the ctime gets set to. I don't think my filesystem is to blame.
* Files larger than just under 16MB, due to the maximum document size in MongoDB.
I now check for this and return EFBIG from ftruncate/truncate/write.
The solution to this is [GridFS](https://docs.mongodb.com/manual/core/gridfs/).
* I don't know if it works on OSes other than Linux, I haven't tried.
* Performance is probably pants, due to things like lack of caching,
and storing the data itself within the inode document.
