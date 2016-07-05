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
  chkaccess:   chkaccess,
  // MongoJS database object
  db:          ({} /*:Object*/), // Filled in by main()
  doMknod:     doMknod,
  igetattr:    igetattr,
  iremove:     iremove,
  itruncate:   itruncate,
  // Manages open file descriptors
  openFiles:   {
    next: 10,
    add:  function (data /*:{inode:string,flags:number}*/) {
      var fd   = this.next++;
      this[fd] = data;
      return fd;
    }
  },
  resolvePath: resolvePath,
  useringroup: useringroup
};
var mf = module.exports;

// Imports
var async   = require('async');
var fuse    = require('fuse-bindings');
var mongojs = require('mongojs');
var posix   = require('posix');

/**
 * Create an inode and a corresponding directory entry from the provided data
 *
 * @param   {String}   path
 * @param   {Object}   inode
 * @param   {Function} cb
 * @returns {undefined}
 */
function doMknod(path /*:string*/, inode /*:Object*/, cb /*:function*/) {
  // Look up the directory the new file will go in
  var pathmod = require('path');
  mf.resolvePath(pathmod.dirname(path), function (err, dirent) {
    if (err) { return cb(err); }
    // Create the inode for the new file (we need its _id for the directory)
    mf.db.inodes.insert(inode, function (err, doc) {
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
 * Takes a canonical path, relative to the root of our fs but with leading /,
 * runs callback with its directory entry
 *
 * @param   {String}   path
 * @param   {Function} cb
 * @returns {undefined}
 */
function resolvePath(path /*:string*/, cb /*:function*/) {
  // Split path into components
  // Note that splitting "/" on "/" gives two empty-string components, we don't want that
  if (path === "/") { path = ""; }
  var names = path.split("/");

  // Create a series of steps to look up each component of the path in turn
  var tasks = names.map(function (name /*:string*/) {
    return function (pdoc, cb) {
      // Look up one component of the path in the directory, return its entry
      // The root is represented with a null parent and an empty-string name
      mf.db.directory.findOne({ name: name, parent: pdoc._id }, function (err, doc) {
        if (err)  { return cb(fuse.EIO,    null); }
        if (!doc) { return cb(fuse.ENOENT, null); }
        cb(null, doc);
      });
    };
  });
  // The first task in a waterfall doesn't get any args apart from the callback
  tasks.unshift(function (cb) { cb(null, { _id: null }); });

  async.waterfall(tasks, cb);
}

/**
 * Truncates the file with the specified inode ID to the specified size
 *
 * @param   {String}   inode
 * @param   {Number}   size
 * @param   {Function} cb
 * @returns {undefined}
 */
function itruncate(inode /*:string*/, size /*:number*/, cb /*:function*/) {
  // Look up that inode
  mf.db.inodes.findOne({ _id: inode }, function (err, doc) {
    if (err)  { return cb(fuse.EIO); }
    if (!doc) { return cb(fuse.ENOENT); }
    // Permissions?
    var access = mf.chkaccess(doc, 0x2);
    if (access) { return cb(access); }
    // Note MongoDB's max document size.
    // This leaves a bit of space for the rest of the inode data.
    if (size > 16000000) { return cb(fuse.EFBIG); }

    // Truncate to the requested size, by copying into a buffer of that size
    var buf = new Buffer(size).fill(0);
    if (doc.data) { doc.data.read(0, size).copy(buf); }
    // Update the inode
    var set = {
      ctime: Date.now(),
      mtime: Date.now(),
      data:  new mongojs.Binary(buf, 0)
    };

    mf.db.inodes.update({ _id: inode }, { $set: set }, function (err, result) {
      if (err || !result.ok || !result.n) { return cb(fuse.EIO); }
      cb(0);
    });
  });
}

/**
 * Gets the attributes of the file with the specified inode ID
 *
 * @param   {String}   inode
 * @param   {Function} cb
 * @returns {undefined}
 */
function igetattr(inode /*:string*/, cb /*:function*/) {
  // Look up that inode
  mf.db.inodes.findOne({ _id: inode }, function (err, doc) {
    if (err)  { return cb(fuse.EIO); }
    if (!doc) { return cb(fuse.ENOENT); }
    // Find and store the size if we didn't already have it
    // However we've still retrieved the data when we only really want the attributes
    if (doc.data && doc.size === undefined) {
      doc.size = doc.data.length();
      mf.db.inodes.update({ _id: inode }, { $set: { size: doc.size } }, function (err, result) {});
    }

    // Look up refcount, required to support hardlinks
    mf.db.directory.count({ inode: inode }, function (err, cnt) {
      if (err) { return cb(fuse.EIO); }
      doc.nlink = cnt;
      cb(0, doc);
    });
  });
}

/**
 * Given an inode ID, remove it if it is not open and has no links.
 *
 * @param   {String}   inode
 * @param   {Function} cb
 * @returns {undefined}
 */
function iremove(inode /*:string*/, cb /*:function*/) {
  // Is it open?
  for (var fd in mf.openFiles) {
    if (mf.openFiles[fd].inode === inode) { return cb(0); }
  }

  // Look up refcount
  mf.db.directory.count({ inode: inode }, function (err, cnt) {
    // Does it have any links?
    // note: is this a race condition (TOCTOU)?
    if (err) { return cb(fuse.EIO); }
    if (cnt) { return cb(0); }

    // If not, remove the inode
    mf.db.inodes.remove({ _id: inode }, true, function (err, res) {
      if (err) { return cb(fuse.EIO); }
      cb(0);
    });
  });
}

/**
 * Check whether the specified user is in the specified group
 *
 * @param   {Number} uid
 * @param   {Number} gid
 * @returns {Boolean}
 */
function useringroup(uid /*:number*/, gid /*:number*/) {
  var pwnam = posix.getpwnam(uid);
  var grnam = posix.getgrnam(gid);
  // ie. was anything returned for the above?
  if (!pwnam.name || !grnam.name) { return false; }
  return grnam.members.indexOf(pwnam.name) !== -1;
}

/**
 * Check a requested permission (and the requesting user) against an inode
 *
 * @param   {Object} inode
 * @param   {Number} mode
 * @returns {Number}
 */
function chkaccess(inode /*:{mode:number,uid:number,gid:number}*/, mode /*:number*/) {
  var context = fuse.context();
  if (mode < 0 || mode > 7) {
    // Invalid access mode
    return fuse.EINVAL;
  } else if (!mode) {
    // If mode is 0, we're just testing existence, and we've proved that
    return 0;
  } else if (!context.uid) {
    // User requesting access is root
    return 0;

  } else if (context.uid === inode.uid) {
    // User requesting access is this file's owner
    var ubits  = (inode.mode >> 6) & 7;
    var umatch = ((ubits & mode) === mode);
    return umatch ? 0 : fuse.EACCES;
  } else if (mf.useringroup(context.uid, inode.gid)) {
    // User requesting access is in this file's group
    var gbits  = (inode.mode >> 3) & 7;
    var gmatch = ((gbits & mode) === mode);
    return gmatch ? 0 : fuse.EACCES;
  } else {
    // Neither user nor group match, check world permissions
    var obits  = (inode.mode >> 0) & 7;
    var omatch = ((obits & mode) === mode);
    return omatch ? 0 : fuse.EACCES;
  }
}
