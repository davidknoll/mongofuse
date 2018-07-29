/**
 * A FUSE filesystem, backed by MongoDB and written in Node.js.
 * Mostly an exercise for me to learn MongoDB and Node.js.
 *
 * @author  David Knoll <david@davidknoll.me.uk>
 * @license MIT
 * @file
 * @flow
 */
'use strict';

// Exports
module.exports = {
  access,
  chmod,
  chown,
  fgetattr,
  ftruncate,
  getattr,
  getxattr,
  init,
  link,
  listxattr,
  mkdir,
  mknod,
  open,
  opendir: open,
  read,
  readdir,
  readlink,
  release,
  releasedir: release,
  removexattr,
  rename,
  rmdir,
  setxattr,
  symlink,
  truncate,
  unlink,
  utimens,
  write
};

// Imports
const async   = require('async');
const fuse    = require('fuse-bindings');
const mongojs = require('mongojs');
const mf      = require('./extra.js');

/**
 * Check permissions before accessing a file
 *
 * @param   {String}   path
 * @param   {Number}   mode
 * @param   {Function} cb
 * @returns {undefined}
 */
function access(path /*:string*/, mode /*:number*/, cb /*:function*/) {
  mf.DEBUG('[access] path %s, mode %d (dec)', path, mode);

  // Look up the requested directory entry
  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }
    // And look up the inode it refers to
    mf.db.inodes.findOne({ _id: dirent.inode }, { data: false }, (err, doc) => {
      if (err)  { return cb(fuse.EIO); }
      if (!doc) { return cb(fuse.ENOENT); }
      // Check the requested permission against the inode
      cb(mf.chkaccess(doc, mode));
    });
  });
}

/**
 * Change file mode
 * Should not be able to do this unless you are root or the existing owner
 *
 * @param   {String}   path
 * @param   {Number}   mode
 * @param   {Function} cb
 * @returns {undefined}
 */
function chmod(path /*:string*/, mode /*:number*/, cb /*:function*/) {
  mf.DEBUG('[chmod] path %s, mode %d (dec)', path, mode);

  // Look up the requested directory entry
  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }
    // And look up the inode it refers to
    mf.db.inodes.findOne({ _id: dirent.inode }, { data: false }, (err, doc) => {
      if (err)  { return cb(fuse.EIO); }
      if (!doc) { return cb(fuse.ENOENT); }

      // In testing, the mode passed in does have the file type bits set,
      // but we probably don't want to go changing them so mask them just in case
      const set = {
        ctime: Date.now(),
        mode:  (doc.mode & 0o170000) | (mode & 0o7777)
      };
      // Is this allowed?
      const context = fuse.context();
      if (context.uid !== 0 && context.uid !== doc.uid) { return cb(fuse.EPERM); }

      // Save changes
      mf.db.inodes.update({ _id: dirent.inode }, { $set: set }, (err, result) => {
        if (err || !result.ok || !result.n) { return cb(fuse.EIO); }
        cb(0);
      });
    });
  });
}

/**
 * Change file owner/group
 * Should not be able to do this unless you are root or the existing owner
 * Should not be able to chown to a user other than yourself, unless you are root
 * Should not be able to chgrp to a group you are not a member of, unless you are root
 *
 * @param   {String}   path
 * @param   {Number}   uid
 * @param   {Number}   gid
 * @param   {Function} cb
 * @returns {undefined}
 */
function chown(path /*:string*/, uid /*:number*/, gid /*:number*/, cb /*:function*/) {
  mf.DEBUG('[chown] path %s, uid %d, gid %d', path, uid, gid);

  // Look up the requested directory entry
  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }
    // And look up the inode it refers to
    mf.db.inodes.findOne({ _id: dirent.inode }, { data: false }, (err, doc) => {
      if (err)  { return cb(fuse.EIO); }
      if (!doc) { return cb(fuse.ENOENT); }

      // If we're setting only uid or only gid, the other will be -1
      const set /*:{uid?:number,gid?:number}*/ = { ctime: Date.now() };
      if (uid >= 0) { set.uid = uid; }
      if (gid >= 0) { set.gid = gid; }
      // Is this allowed?
      const context = fuse.context();
      if (context.uid !== 0 && context.uid !== doc.uid) { return cb(fuse.EPERM); }
      if (context.uid !== 0 && uid !== -1 && context.uid !== uid) { return cb(fuse.EPERM); }
      if (context.uid !== 0 && gid !== -1 && !mf.useringroup(context.uid, gid)) { return cb(fuse.EPERM); }

      // Save changes
      mf.db.inodes.update({ _id: dirent.inode }, { $set: set }, (err, result) => {
        if (err || !result.ok || !result.n) { return cb(fuse.EIO); }
        cb(0);
      });
    });
  });
}

/**
 * Get file attributes, by open file descriptor
 *
 * @param   {String}   path
 * @param   {Number}   fd
 * @param   {Function} cb
 * @returns {undefined}
 */
function fgetattr(path /*:string*/, fd /*:number*/, cb /*:function*/) {
  mf.DEBUG('[fgetattr] path %s, fd %d', path, fd);
  if (!mf.openFiles[fd]) { return cb(fuse.EBADF); }
  mf.igetattr(mf.openFiles[fd].inode, cb);
}

/**
 * Truncate file, by open file descriptor
 *
 * @param   {String}   path
 * @param   {Number}   fd
 * @param   {Number}   size
 * @param   {Function} cb
 * @returns {undefined}
 */
function ftruncate(path /*:string*/, fd /*:number*/, size /*:number*/, cb /*:function*/) {
  mf.DEBUG('[ftruncate] path %s, fd %d, size %d (bytes)', path, fd, size);
  if (!mf.openFiles[fd]) { return cb(fuse.EBADF); }
  mf.itruncate(mf.openFiles[fd].inode, size, cb);
}

/**
 * Get file attributes, by path
 *
 * @param   {String}   path
 * @param   {Function} cb
 * @returns {undefined}
 */
function getattr(path /*:string*/, cb /*:function*/) {
  mf.DEBUG('[getattr] path %s', path);
  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }
    mf.igetattr(dirent.inode, cb);
  });
}

/**
 * Read an extended attribute
 *
 * @param {String}   path
 * @param {String}   name
 * @param {Buffer}   buf
 * @param {Number}   len
 * @param {Number}   off
 * @param {Function} cb
 */
function getxattr(path /*:string*/, name /*:string*/, buf /*:Buffer*/, len /*:number*/, off /*:number*/, cb /*:function*/) {
  mf.DEBUG('[getxattr] path %s, name %s, len %d (bytes), off %d (bytes)', path, name, len, off);
  const bname = mf.b64enc(name);

  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }
    mf.db.inodes.findOne({ _id: dirent.inode }, { xattr: true }, (err, inode) => {
      // Does this extended attribute exist?
      if (err)    { return cb(fuse.EIO); }
      if (!inode) { return cb(fuse.ENOENT); }
      if (!inode.xattr)        { return cb(fuse.ENODATA); }
      if (!inode.xattr[bname]) { return cb(fuse.ENODATA); }

      // If destination buffer is size 0, return the length of the extended attribute.
      // If too small, return an error.
      // Otherwise copy it into place and return the length.
      const xalen = inode.xattr[bname].length();
      if (!len) {
        return cb(xalen);
      } else if (len < xalen) {
        return cb(fuse.ERANGE);

      } else {
        // inode.xattr[name] is a MongoDB "Binary" object. read()ing it gives a Node "Buffer" object.
        const srcbuf = inode.xattr[bname].read(0, len);
        const copied = srcbuf.copy(buf, off);
        return cb(copied);
      }
    });
  });
}

/**
 * Called on filesystem init, sets up a root directory if it's a new instance
 *
 * @param   {Function} cb
 * @returns {undefined}
 */
function init(cb /*:function*/) {
  mf.DEBUG('[init]');

  // Does the root directory exist?
  mf.resolvePath("/", (err, dirent) => {
    if (err === fuse.ENOENT) {
      mf.INFO("Creating root directory");
      // Create the root directory's inode, using details of invoking user
      mf.db.inodes.insert({
        // $FlowIssue argument to umask is optional, see the docs
        mode:  0o040777 & ~process.umask(),
        uid:   process.geteuid ? process.geteuid() : 0, // Only on POSIX platforms
        gid:   process.getegid ? process.getegid() : 0, // Only on POSIX platforms
        rdev:  0,
        ctime: Date.now(),
        mtime: Date.now(),
        atime: Date.now()

      }, (err, doc) => {
        if (err) { return cb(fuse.EIO); }
        // Create the root directory entry, identified by null parent and empty name
        mf.db.directory.insert({
          name:   "",
          parent: null,
          inode:  doc._id

        }, (err, doc) => {
          if (err) { return cb(fuse.EIO); }
          cb(0);
        });
      });
    } else { cb(err); }
  });
}

/**
 * Create a hard link
 *
 * @param   {String}   src
 * @param   {String}   dest
 * @param   {Function} cb
 * @returns {undefined}
 */
function link(src /*:string*/, dest /*:string*/, cb /*:function*/) {
  mf.DEBUG('[link] src %s (target), dest %s (link)', src, dest);
  const path = require('path');
  let target;

  async.waterfall([
    // Look up the target's directory entry, to get its inode number
    acb => mf.resolvePath(src, acb),

    (srcdirent, acb) => {
      target = srcdirent.inode;
      // Look up that inode, to check it's not a directory
      mf.db.inodes.findOne({ _id: target }, { data: false }, acb);
    },

    (srcinode, acb) => {
      if (!srcinode) { return acb(fuse.ENOENT); }
      if ((srcinode.mode & 0o170000) === 0o040000) { return acb(fuse.EPERM); }
      // Look up the new link's parent directory entry
      mf.resolvePath(path.dirname(dest), acb);
    },

    (destpdirent, acb) => {
      // Create the new directory entry
      const newdirent = {
        name:   path.basename(dest),
        parent: destpdirent._id,
        inode:  target
      };
      mf.db.directory.insert(newdirent, acb);
    }

  ], (err, result) => {
    // FUSE errors (from resolvePath), MongoDB errors, success
    if (err < 0) { return cb(err); }
    if (err)     { return cb(fuse.EIO); }
    cb(0);
  });
}

/**
 * List extended attributes
 *
 * @param   {String}   path
 * @param   {Buffer}   buf
 * @param   {Number}   len
 * @param   {Function} cb
 * @returns {undefined}
 */
function listxattr(path /*:string*/, buf /*:Buffer*/, len /*:number*/, cb /*:function*/) {
  mf.DEBUG('[listxattr] path %s, len %d (bytes)', path, len);
  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }
    mf.db.inodes.findOne({ _id: dirent.inode }, { xattr: true }, (err, inode) => {
      // Does this inode have extended attributes?
      if (err)          { return cb(fuse.EIO); }
      if (!inode)       { return cb(fuse.ENOENT); }
      if (!inode.xattr) { return cb(0); }

      const keys = Object.keys(inode.xattr).map(mf.b64dec);
      if (!keys.length) { return cb(0); }
      const result = keys.join('\0') + '\0';

      // If destination buffer is size 0, return the length of buffer required.
      // If too small, return an error.
      // Otherwise copy it into place and return the length.
      if (!len) {
        return cb(result.length);
      } else if (len < result.length) {
        return cb(fuse.ERANGE);
      } else {
        buf.write(result);
        return cb(result.length);
      }
    });
  });
}

/**
 * Create directory
 *
 * @param   {String}   path
 * @param   {Number}   mode
 * @param   {Function} cb
 * @returns {undefined}
 */
function mkdir(path /*:string*/, mode /*:number*/, cb /*:function*/) {
  mf.DEBUG('[mkdir] path %s, mode %d (dec)', path, mode);
  // Set file type bits for a directory, as they aren't supplied here
  mknod(path, (mode & 0o7777) | 0o040000, 0, cb);
}

/**
 * Create file node
 *
 * @param   {String}   path
 * @param   {Number}   mode
 * @param   {Number}   dev
 * @param   {Function} cb
 * @returns {undefined}
 */
function mknod(path /*:string*/, mode /*:number*/, dev /*:number*/, cb /*:function*/) {
  mf.DEBUG('[mknod] path %s, mode %d (dec), dev %d (major*256 + minor)', path, mode, dev);

  // mode includes file type bits, dev is (major << 8) + minor
  // (and is called rdev in the inode / what gets returned by getattr)
  const context = fuse.context();
  mf.doMknod(path, {
    mode:  mode,
    uid:   context.uid,
    gid:   context.gid,
    rdev:  dev,
    ctime: Date.now(),
    mtime: Date.now(),
    atime: Date.now()
  }, cb);
}

/**
 * Open file
 *
 * @param   {String}   path
 * @param   {Number}   flags
 * @param   {Function} cb
 * @returns {undefined}
 */
function open(path /*:string*/, flags /*:number*/, cb /*:function*/) {
  mf.DEBUG('[open] path %s, flags %d (dec)', path, flags);

  // If this is a new file it will already have been created by mknod
  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }
    // Look up the inode...
    mf.db.inodes.findOne({ _id: dirent.inode }, { data: false }, (err, doc) => {
      if (err)  { return cb(fuse.EIO); }
      if (!doc) { return cb(fuse.ENOENT); }

      // ...in order to check permissions
      const modemap = [ 4, 2, 6, -1 ];
      const access  = mf.chkaccess(doc, modemap[flags & 0x3]);
      if (access) { return cb(access); }
      // Was O_DIRECTORY specified?
      if ((flags & 0o200000) && ((doc.mode & 0o170000) !== 0o040000)) {
        return cb(fuse.ENOTDIR);
      }

      // Add it to the list of open file descriptors
      const fd = mf.openFiles.add({
        inode: dirent.inode,
        flags: flags
      });
      cb(0, fd);
    });
  });
}

/**
 * Read data from a file
 *
 * @param   {String}   path
 * @param   {Number}   fd
 * @param   {Buffer}   buf
 * @param   {Number}   len
 * @param   {Number}   pos
 * @param   {Function} cb
 * @returns {undefined}
 */
function read(path /*:string*/, fd /*:number*/, buf /*:Buffer*/, len /*:number*/, pos /*:number*/, cb /*:function*/) {
  mf.DEBUG('[read] path %s, fd %d, len %d (bytes), pos %d (bytes)', path, fd, len, pos);

  // Is it open for reading?
  if (!mf.openFiles[fd]) { return cb(fuse.EBADF); }
  if ((mf.openFiles[fd].flags & 0x3) === 0x1) { return cb(fuse.EBADF); }

  // Look up the inode of the open file
  mf.db.inodes.findOne({ _id: mf.openFiles[fd].inode }, (err, doc) => {
    if (err)  { return cb(fuse.EIO); }
    if (!doc) { return cb(fuse.ENOENT); }
    if ((doc.mode & 0o170000) === 0o040000) { return cb(fuse.EISDIR); }

    // Does the atime need updating?
    mf.chkatime(doc, err => {
      if (err)       { return cb(fuse.EIO); }
      if (!doc.data) { return cb(0); }
      // doc.data is a MongoDB "Binary" object. read()ing it gives a Node "Buffer" object.
      const srcbuf = doc.data.read(pos, len);
      const copied = srcbuf.copy(buf);
      return cb(copied);
    });
  });
}

/**
 * List entries in a directory
 *
 * @param   {String}   path
 * @param   {Function} cb
 * @returns {undefined}
 */
function readdir(path /*:string*/, cb /*:function*/) {
  mf.DEBUG('[readdir] path %s', path);

  // Look up the requested directory itself
  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }

    // Does the directory atime need updating?
    if (global.ATIME_LEVEL) {
      mf.db.inodes.findOne({ _id: dirent.inode }, { data: false }, (err, inode) => {
        if (err) { return cb(fuse.EIO); }
        mf.chkatime(inode, err => {
          if (err) { return cb(fuse.EIO); }
          readdir_inner();
        });
      });
    } else {
      readdir_inner();
    }

    function readdir_inner() {
      // Look up children of the requested directory
      mf.db.directory.find({ parent: dirent._id }, (err, docs) => {
        if (err) { return cb(fuse.EIO); }
        const names = docs.map(cdir => cdir.name);
        // According to POSIX we're only meant to return . and .. if the entries actually exist,
        // but if we don't they won't appear in a directory listing.
        // We don't need to process them further ourselves- the paths we get are already canonicalised.
        names.unshift('..');
        names.unshift('.');
        cb(0, names);
      });
    }
  });
}

/**
 * Resolve a symbolic link
 *
 * @param   {String}   path
 * @param   {Function} cb
 * @returns {undefined}
 */
function readlink(path /*:string*/, cb /*:function*/) {
  mf.DEBUG('[readlink] path %s', path);

  // Look up the requested directory entry
  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }
    // And look up the inode it refers to
    mf.db.inodes.findOne({ _id: dirent.inode }, (err, doc) => {
      if (err)  { return cb(fuse.EIO); }
      if (!doc) { return cb(fuse.ENOENT); }
      if ((doc.mode & 0o170000) !== 0o120000) { return cb(fuse.EINVAL); }
      // Does the atime of the symlink itself need updating?
      mf.chkatime(doc, err => {
        if (err) { return cb(fuse.EIO); }
        // Get the target from the data, stored in the inode
        cb(0, doc.data.value());
      });
    });
  });
}

/**
 * Close a file descriptor
 *
 * @param   {String}   path
 * @param   {Number}   fd
 * @param   {Function} cb
 * @returns {undefined}
 */
function release(path /*:string*/, fd /*:number*/, cb /*:function*/) {
  mf.DEBUG('[release] path %s, fd %d', path, fd);

  // If an open file, close it
  if (!mf.openFiles[fd]) { return cb(fuse.EBADF); }
  const inode = mf.openFiles[fd].inode;
  delete mf.openFiles[fd];

  // Remove the inode if it has no more references
  mf.iremove(inode, cb);
}

/**
 * Remove extended attribute
 *
 * @param   {String}   path
 * @param   {String}   name
 * @param   {Function} cb
 * @returns {undefined}
 */
function removexattr(path /*:string*/, name /*:string*/, cb /*:function*/) {
  mf.DEBUG('[removexattr] path %s, name %s', path, name);
  const bname = mf.b64enc(name);

  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }
    mf.db.inodes.findOne({ _id: dirent.inode }, { xattr: true }, (err, inode) => {
      // Does this extended attribute exist?
      if (err)    { return cb(fuse.EIO); }
      if (!inode) { return cb(fuse.ENOENT); }
      // Supposedly ENODATA on Linux but ENOATTR on OS X,
      // but fuse-bindings doesn't define ENOATTR.
      if (!inode.xattr)        { return cb(fuse.ENODATA); }
      if (!inode.xattr[bname]) { return cb(fuse.ENODATA); }

      // Unset the field within the xattr sub-document
      const unset = {
        [ 'xattr.' + bname ]: 1
      };
      mf.db.inodes.update({ _id: dirent.inode }, { $unset: unset }, (err, result) => {
        if (err) { return cb(fuse.EIO); }
        return cb(0);
      });
    });
  });
}

/**
 * Rename a file
 *
 * @param   {String}   src
 * @param   {String}   dest
 * @param   {Function} cb
 * @returns {undefined}
 */
function rename(src /*:string*/, dest /*:string*/, cb /*:function*/) {
  mf.DEBUG('[rename] src %s, dest %s', src, dest);
  const pathmod = require('path');
  let dirent;

  async.waterfall([
    // Look up the directory entry being renamed
    acb => mf.resolvePath(src, acb),

    (doc, acb) => {
      // Look up the directory the renamed file will go in
      dirent = doc;
      mf.resolvePath(pathmod.dirname(dest), acb);
    },

    (pdirent, acb) => {
      // Set the new name and parent
      const set = {
        name:   pathmod.basename(dest),
        parent: pdirent._id
      };
      mf.db.directory.update({ _id: dirent._id }, { $set: set }, acb);
    }

  ], (err, result) => {
    // FUSE errors (from resolvePath), MongoDB errors, success
    if (err < 0) { return cb(err); }
    if (err)     { return cb(fuse.EIO); }
    cb(0);
  });
}

/**
 * Remove empty directory (if empty, else return error)
 *
 * @param   {String}   path
 * @param   {Function} cb
 * @returns {undefined}
 */
function rmdir(path /*:string*/, cb /*:function*/) {
  mf.DEBUG('[rmdir] path %s', path);

  // Look up the directory being deleted
  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }
    // See if it has any children, and if not, delete it
    mf.db.directory.count({ parent: dirent._id }, (err, cnt) => {
      if (err) { return cb(fuse.EIO); }
      if (cnt) { return cb(fuse.ENOTEMPTY); }
      unlink(path, cb);
    });
  });
}

/**
 * Write an extended attribute
 *
 * @param {String}   path
 * @param {String}   name
 * @param {Buffer}   buf
 * @param {Number}   len
 * @param {Number}   off
 * @param {Number}   flags
 * @param {Function} cb
 */
function setxattr(path /*:string*/, name /*:string*/, buf /*:Buffer*/, len /*:number*/, off /*:number*/, flags /*:number*/, cb /*:function*/) {
  mf.DEBUG('[setxattr] path %s, name %s, len %d (bytes), off %d (bytes), flags %d (dec)', path, name, len, off, flags);
  const bname = mf.b64enc(name);

  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }

    // Make sure we have a buffer of exactly the size of the write data
    const dstbuf = new Buffer(len);
    dstbuf.fill(0);
    buf.copy(dstbuf, 0, off, off + len);

    // And write just that value to the db
    const set = {
      [ 'xattr.' + bname ]: new mongojs.Binary(dstbuf)
    };
    mf.db.inodes.update({ _id: dirent.inode }, { $set: set }, (err, result) => {
      if (err) { return cb(fuse.EIO); }
      return cb(0);
    });
  });
}

/**
 * Create a symbolic link
 *
 * @param   {String}   src
 * @param   {String}   dest
 * @param   {Function} cb
 * @returns {undefined}
 */
function symlink(src /*:string*/, dest /*:string*/, cb /*:function*/) {
  mf.DEBUG('[symlink] src %s (target), dest %s (link)', src, dest);

  // mknod with mode set for a symlink, the target being stored as if file data
  const context = fuse.context();
  mf.doMknod(dest, {
    mode:  0o120777,
    uid:   context.uid,
    gid:   context.gid,
    ctime: Date.now(),
    mtime: Date.now(),
    atime: Date.now(),
    data:  new mongojs.Binary(new Buffer(src), 0)
  }, cb);
}

/**
 * Truncate file, by path
 *
 * @param   {String}   path
 * @param   {Number}   size
 * @param   {Function} cb
 * @returns {undefined}
 */
function truncate(path /*:string*/, size /*:number*/, cb /*:function*/) {
  mf.DEBUG('[truncate] path %s, size %d (bytes)', path, size);
  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }
    mf.itruncate(dirent.inode, size, cb);
  });
}

/**
 * Delete file
 *
 * @param   {String}   path
 * @param   {Function} cb
 * @returns {undefined}
 */
function unlink(path /*:string*/, cb /*:function*/) {
  mf.DEBUG('[unlink] path %s', path);

  let dirent;
  async.waterfall([
    // Look up the requested directory entry
    acb => mf.resolvePath(path, acb),

    (doc, acb) => {
      // And remove it, now we know its inode
      dirent = doc;
      mf.db.directory.remove({ _id: dirent._id }, true, acb);
    },

    (doc, acb) => {
      // Remove the inode if safe
      mf.iremove(dirent.inode, acb);
    }

  ], err => {
    // FUSE errors (from resolvePath), MongoDB errors, success
    if (err < 0) { return cb(err); }
    if (err)     { return cb(fuse.EIO); }
    cb(0);
  });
}

/**
 * Update file access and modify time
 *
 * @param   {String}   path
 * @param   {Date}     atime
 * @param   {Date}     mtime
 * @param   {Function} cb
 * @returns {undefined}
 */
function utimens(path /*:string*/, atime /*:Date*/, mtime /*:Date*/, cb /*:function*/) {
  mf.DEBUG('[utimens] path %s, atime %d (ms), mtime %d (ms)', path, atime, mtime);

  // Look up the requested directory entry
  mf.resolvePath(path, (err, dirent) => {
    if (err) { return cb(err); }
    // And look up the inode it refers to
    mf.db.inodes.findOne({ _id: dirent.inode }, { data: false }, (err, doc) => {
      if (err)  { return cb(fuse.EIO); }
      if (!doc) { return cb(fuse.ENOENT); }
      // Store times as UNIX timestamps in milliseconds
      // If doing touch -a or touch -m, the other time still gets passed
      // For some strange reason, when testing this by repeatedly touching a file,
      // the changing time(s) getting passed in here were jumping about randomly over 15min or so
      const set = {
        atime: atime.getTime(),
        mtime: mtime.getTime(),
        ctime: Date.now()
      };
      mf.db.inodes.update({ _id: dirent.inode }, { $set: set }, (err, result) => {
        if (err || !result.ok || !result.n) { return cb(fuse.EIO); }
        cb(0);
      });
    });
  });
}

/**
 * Write data to a file
 *
 * @param   {String}   path
 * @param   {Number}   fd
 * @param   {Buffer}   buf
 * @param   {Number}   len
 * @param   {Number}   pos
 * @param   {Function} cb
 * @returns {undefined}
 */
function write(path /*:string*/, fd /*:number*/, buf /*:Buffer*/, len /*:number*/, pos /*:number*/, cb /*:function*/) {
  mf.DEBUG('[write] path %s, fd %d, len %d (bytes), pos %d (bytes)', path, fd, len, pos);

  // Is it open for writing?
  if (!mf.openFiles[fd]) { return cb(fuse.EBADF); }
  if ((mf.openFiles[fd].flags & 0x3) === 0x0) { return cb(fuse.EBADF); }

  // Look up the inode of the open file
  mf.db.inodes.findOne({ _id: mf.openFiles[fd].inode }, (err, doc) => {
    if (err)  { return cb(fuse.EIO); }
    if (!doc) { return cb(fuse.ENOENT); }
    if ((doc.mode & 0o170000) === 0o040000) { return cb(fuse.EISDIR); }

    // Make sure we have a buffer of exactly the size of the write data
    const dstbuf = new Buffer(len);
    dstbuf.fill(0);
    const copied = buf.copy(dstbuf, 0, 0, len);
    if (!doc.data) { doc.data = new mongojs.Binary(new Buffer(0), 0); }
    doc.data.write(dstbuf, pos);
    // Note MongoDB's max document size.
    // This leaves a bit of space for the rest of the inode data.
    if (doc.data.length() > 16000000) { return cb(fuse.EFBIG); }
    // Update the inode (yes we're storing the data with the inode right now)
    const set = {
      ctime: Date.now(),
      mtime: Date.now(),
      size:  doc.data.length(),
      data:  doc.data
    };

    mf.db.inodes.update({ _id: mf.openFiles[fd].inode }, { $set: set }, (err, result) => {
      if (err || !result.ok || !result.n) { return cb(fuse.EIO); }
      cb(copied);
    });
  });
}
