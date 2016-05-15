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
  chmod:     chmod,
  chown:     chown,
  fgetattr:  fgetattr,
  ftruncate: ftruncate,
  getattr:   getattr,
  mkdir:     mkdir,
  mknod:     mknod,
  open:      open,
  read:      read,
  readdir:   readdir,
  rmdir:     rmdir,
  symlink:   symlink,
  truncate:  truncate,
  unlink:    unlink,
  utimens:   utimens,
  write:     write
};

// Imports
var fuse    = require('fuse-bindings');
var mongojs = require('mongojs');
var mf      = require('./extra.js');

/**
 * Change file mode
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
      mf.db.inodes.update({ _id: dirent.inode }, { $set: set }, function (err, result) {
        if (err || !result.ok || !result.n) { return cb(fuse.EIO); }
        cb(0);
      });
    });
  });
}

/**
 * Change file owner/group
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
 * Create directory
 *
 * @param   {String}   path
 * @param   {Number}   mode
 * @param   {Function} cb
 * @returns {undefined}
 */
function mkdir(path /*:string*/, mode /*:number*/, cb /*:function*/) {
  mknod(path, mode | 0040000, 0, cb);
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
  var pathmod = require('path');
  // Look up the directory the new file will go in
  mf.resolvePath(pathmod.dirname(path), function (err, dirent) {
    if (err) { return cb(err); }
    // Create the inode for the new file (we need its _id for the directory)
    var context = fuse.context();
    var newinode = {
      mode:  mode,
      uid:   context.uid,
      gid:   context.gid,
      rdev:  dev,
      ctime: Date.now()
    };
    mf.db.inodes.insert(newinode, function (err, doc) {
      if (err) { return cb(fuse.EIO); }
      // Create the new directory entry
      var newdir = {
        name:   pathmod.basename(path),
        parent: dirent._id,
        inode:  doc._id
      };
      mf.db.directory.insert(newdir, function (err, doc) {
        if (err) { return cb(fuse.EIO); }
        cb(0);
      });
    });
  });
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
  // Not yet implemented
  console.log("symlink src " + src + " dest " + dest);
  //mknod(dest, 0120777, 0, cb);
  cb(fuse.ENFILE);
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
  // todo: use async for this callback hell
  // Look up the requested directory entry
  mf.resolvePath(path, function (err, dirent) {
    if (err) { return cb(err); }
    // And remove it, now we know its inode
    mf.db.directory.remove({ _id: dirent._id }, true, function (err, doc) {
      if (err) { return cb(fuse.EIO); }
      // And look up the refcount of that inode
      mf.db.directory.count({ inode: dirent.inode }, function (err, cnt) {
        if (err) { return cb(fuse.EIO); }
        if (cnt) { return cb(0); }
        // And if it's zero delete the inode
        // note: is this a race condition?
        mf.db.inodes.remove({ _id: dirent.inode }, true, function (err, doc) {
          if (err) { return cb(fuse.EIO); }
          cb(0);
        });
      });
    });
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
