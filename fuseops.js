/**
 * A FUSE filesystem, backed by MongoDB and written in Node.js.
 * Mostly an exercise for me to learn MongoDB and Node.js.
 *
 * @author  David Knoll <david@davidknoll.me.uk>
 * @license MIT
 * @file
 * @flow
 */

// Exports
module.exports = {
  access:    access,
  chmod:     chmod,
  chown:     chown,
  fgetattr:  fgetattr,
  ftruncate: ftruncate,
  getattr:   getattr,
  init:      init,
  mkdir:     mkdir,
  mknod:     mknod,
  open:      open,
  read:      read,
  readdir:   readdir,
  readlink:  readlink,
  release:   release,
  rename:    rename,
  rmdir:     rmdir,
  symlink:   symlink,
  truncate:  truncate,
  unlink:    unlink,
  utimens:   utimens,
  write:     write
};

// Imports
var async   = require('async');
var fuse    = require('fuse-bindings');
var mongojs = require('mongojs');
var mf      = require('./extra.js');

/**
 * Check permissions before accessing a file
 *
 * @param   {String}   path
 * @param   {Number}   mode
 * @param   {Function} cb
 * @returns {undefined}
 */
function access(path /*:string*/, mode /*:number*/, cb /*:function*/) {
  // Look up the requested directory entry
  mf.resolvePath(path, function (err, dirent) {
    if (err) { return cb(err); }
    // And look up the inode it refers to
    mf.db.inodes.findOne({ _id: dirent.inode }, function (err, doc) {
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
  // Look up the requested directory entry
  mf.resolvePath(path, function (err, dirent) {
    if (err) { return cb(err); }
    // And look up the inode it refers to
    mf.db.inodes.findOne({ _id: dirent.inode }, function (err, doc) {
      if (err)  { return cb(fuse.EIO); }
      if (!doc) { return cb(fuse.ENOENT); }

      // In testing, the mode passed in does have the file type bits set,
      // but we probably don't want to go changing them so mask them just in case
      var set = {
        ctime: Date.now(),
        mode:  (doc.mode & 0170000) | (mode & 07777)
      };
      // Is this allowed?
      var context = fuse.context();
      if (context.uid !== 0 && context.uid !== doc.uid) { return cb(fuse.EPERM); }

      // Save changes
      mf.db.inodes.update({ _id: dirent.inode }, { $set: set }, function (err, result) {
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
  // Look up the requested directory entry
  mf.resolvePath(path, function (err, dirent) {
    if (err) { return cb(err); }
    // And look up the inode it refers to
    mf.db.inodes.findOne({ _id: dirent.inode }, function (err, doc) {
      if (err)  { return cb(fuse.EIO); }
      if (!doc) { return cb(fuse.ENOENT); }

      // If we're setting only uid or only gid, the other will be -1
      var set /*:{uid?:number,gid?:number}*/ = { ctime: Date.now() };
      if (uid >= 0) { set.uid = uid; }
      if (gid >= 0) { set.gid = gid; }
      // Is this allowed?
      var context = fuse.context();
      if (context.uid !== 0 && context.uid !== doc.uid) { return cb(fuse.EPERM); }
      if (context.uid !== 0 && context.uid !== uid)     { return cb(fuse.EPERM); }
      if (context.uid !== 0 && !mf.useringroup(context.uid, gid)) { return cb(fuse.EPERM); }

      // Save changes
      mf.db.inodes.update({ _id: dirent.inode }, { $set: set }, function (err, result) {
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
  mf.resolvePath(path, function (err, dirent) {
    if (err) { return cb(err); }
    mf.igetattr(dirent.inode, cb);
  });
}

/**
 * Called on filesystem init, sets up a root directory if it's a new instance
 *
 * @param   {Function} cb
 * @returns {undefined}
 */
function init(cb /*:function*/) {
  // Does the root directory exist?
  mf.resolvePath("/", function (err, dirent) {
    if (err === fuse.ENOENT) {
      console.log("Creating root directory");
      // Create the root directory's inode, using details of invoking user
      mf.db.inodes.insert({
        mode:  0040777 & ~process.umask(),
        uid:   process.geteuid(),
        gid:   process.getegid(),
        ctime: Date.now(),
        mtime: Date.now(),
        atime: Date.now()

      }, function (err, doc) {
        if (err) { return cb(fuse.EIO); }
        // Create the root directory entry, identified by null parent and empty name
        mf.db.directory.insert({
          name:   "",
          parent: null,
          inode:  doc._id

        }, function (err, doc) {
          if (err) { return cb(fuse.EIO); }
          cb(0);
        });
      });
    } else { cb(err); }
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
  // Set file type bits for a directory, as they aren't supplied here
  mknod(path, (mode & 07777) | 0040000, 0, cb);
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
  // mode includes file type bits, dev is (major << 8) + minor
  // (and is called rdev in the inode / what gets returned by getattr)
  var context = fuse.context();
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
  // If this is a new file it will already have been created by mknod
  mf.resolvePath(path, function (err, dirent) {
    if (err) { return cb(err); }
    // Add it to the list of open file descriptors
    var fd = mf.openFiles.add({
      inode: dirent.inode,
      flags: flags
    });
    cb(0, fd);
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
  if (!mf.openFiles[fd]) { return cb(fuse.EBADF); }
  // Look up the inode of the open file
  mf.db.inodes.findOne({ _id: mf.openFiles[fd].inode }, function (err, doc) {
    if (err)  { return cb(fuse.EIO); }
    if (!doc) { return cb(fuse.ENOENT); }
    if (!doc.data) { return cb(0); }
    // doc.data is a MongoDB "Binary" object. read()ing it gives a Node "Buffer" object.
    var srcbuf = doc.data.read(pos, len);
    var copied = srcbuf.copy(buf);
    cb(copied);
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
  // Look up the requested directory itself
  mf.resolvePath(path, function (err, dirent) {
    if (err) { return cb(err); }
    // Look up children of the requested directory
    mf.db.directory.find({ parent: dirent._id }, function (err, docs) {
      if (err) { return cb(fuse.EIO); }
      var names = docs.map(function (cdir) { return cdir.name; });
      // According to POSIX we're only meant to return . and .. if the entries actually exist,
      // but if we don't they won't appear in a directory listing.
      // We don't need to process them further ourselves- the paths we get are already canonicalised.
      names.unshift('..');
      names.unshift('.');
      cb(0, names);
    });
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
  // Look up the requested directory entry
  mf.resolvePath(path, function (err, dirent) {
    if (err) { return cb(err); }
    // And look up the inode it refers to
    mf.db.inodes.findOne({ _id: dirent.inode }, function (err, doc) {
      if (err)  { return cb(fuse.EIO); }
      if (!doc) { return cb(fuse.ENOENT); }
      // Get the target from it
      cb(0, doc.data.value());
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
  // If an open file, close it
  if (!mf.openFiles[fd]) { return cb(fuse.EBADF); }
  var inode = mf.openFiles[fd].inode;
  delete mf.openFiles[fd];

  // Remove the inode if it has no more references
  mf.iremove(inode, cb);
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
  var pathmod = require('path');
  var dirent;

  async.waterfall([
    function (acb) {
      // Look up the directory entry being renamed
      mf.resolvePath(src, acb);
    },

    function (doc, acb) {
      // Look up the directory the renamed file will go in
      dirent = doc;
      mf.resolvePath(pathmod.dirname(dest), acb);
    },

    function (pdirent, acb) {
      // Set the new name and parent
      var set = {
        name:   pathmod.basename(dest),
        parent: pdirent._id
      };
      mf.db.directory.update({ _id: dirent._id }, { $set: set }, acb);
    }

  ], function (err, result) {
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
  // Look up the directory being deleted
  mf.resolvePath(path, function (err, dirent) {
    if (err) { return cb(err); }
    // See if it has any children, and if not, delete it
    mf.db.directory.count({ parent: dirent._id }, function (err, cnt) {
      if (err) { return cb(fuse.EIO); }
      if (cnt) { return cb(fuse.ENOTEMPTY); }
      unlink(path, cb);
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
  // mknod with mode set for a symlink, the target being stored as if file data
  var context = fuse.context();
  mf.doMknod(dest, {
    mode:  0120777,
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
  mf.resolvePath(path, function (err, dirent) {
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
  var dirent;
  async.waterfall([
    function (acb) {
      // Look up the requested directory entry
      mf.resolvePath(path, acb);
    },

    function (doc, acb) {
      // And remove it, now we know its inode
      dirent = doc;
      mf.db.directory.remove({ _id: dirent._id }, true, acb);
    },

    function (doc, acb) {
      // Remove the inode if safe
      mf.iremove(dirent.inode, acb);
    }

  ], function (err) {
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
  // Look up the requested directory entry
  mf.resolvePath(path, function (err, dirent) {
    if (err) { return cb(err); }
    // And look up the inode it refers to
    mf.db.inodes.findOne({ _id: dirent.inode }, function (err, doc) {
      if (err)  { return cb(fuse.EIO); }
      if (!doc) { return cb(fuse.ENOENT); }
      // Store times as UNIX timestamps in milliseconds
      // If doing touch -a or touch -m, the other time still gets passed
      // For some strange reason, when testing this by repeatedly touching a file,
      // the changing time(s) getting passed in here were jumping about randomly over 15min or so
      var set = {
        atime: atime.getTime(),
        mtime: mtime.getTime(),
        ctime: Date.now()
      };
      mf.db.inodes.update({ _id: dirent.inode }, { $set: set }, function (err, result) {
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
  if (!mf.openFiles[fd]) { return cb(fuse.EBADF); }
  // Look up the inode of the open file
  mf.db.inodes.findOne({ _id: mf.openFiles[fd].inode }, function (err, doc) {
    if (err)  { return cb(fuse.EIO); }
    if (!doc) { return cb(fuse.ENOENT); }
    // Make sure we have a buffer of exactly the size of the write data
    var dstbuf = new Buffer(len);
    dstbuf.fill(0);
    var copied = buf.copy(dstbuf, 0, 0, len);
    if (!doc.data) { doc.data = new mongojs.Binary(new Buffer(0), 0); }
    doc.data.write(dstbuf, pos);
    // Update the inode (yes we're storing the data with the inode right now,
    // which will break if it causes the inode to exceed the max document size)
    var set = {
      ctime: Date.now(),
      mtime: Date.now(),
      data:  doc.data
    };
    mf.db.inodes.update({ _id: mf.openFiles[fd].inode }, { $set: set }, function (err, result) {
      if (err || !result.ok || !result.n) { return cb(fuse.EIO); }
      cb(copied);
    });
  });
}
