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
  b64dec,
  b64enc,
  chkaccess,
  chkatime,
  // MongoJS database object
  db: ({} /*:Object*/), // Filled in by main()
  doMknod,
  igetattr,
  iremove,
  itruncate,
  // Manages open file descriptors
  openFiles: {
    next: 10,
    add:  function (data /*:{inode:string,flags:number}*/) {
      const fd = this.next++;
      this[fd] = data;
      mf.DEBUG('fd %d opened', fd);
      return fd;
    }
  },
  resolvePath,
  useringroup,

  // https://www.npmjs.com/package/yargs#yargs-even-counts-your-booleans
  WARN:  function WARN(...args /*:any[]*/)  { global.VERBOSE_LEVEL >= 0 && console.log.apply(console, arguments); },
  INFO:  function INFO(...args /*:any[]*/)  { global.VERBOSE_LEVEL >= 1 && console.log.apply(console, arguments); },
  DEBUG: function DEBUG(...args /*:any[]*/) { global.VERBOSE_LEVEL >= 2 && console.log.apply(console, arguments); }
};
const mf = module.exports;

// Imports
const async   = require('async');
const fuse    = require('fuse-bindings');
const mongojs = require('mongojs');
const posix   = require('posix');

/**
 * Create an inode and a corresponding directory entry from the provided data
 *
 * @param   {String}   path
 * @param   {Object}   inode
 * @param   {Function} cb
 * @returns {undefined}
 */
function doMknod(path /*:string*/, inode /*:Object*/, cb /*:function*/) {
  mf.DEBUG('Create %s', path);

  // Look up the directory the new file will go in
  const pathmod = require('path');
  mf.resolvePath(pathmod.dirname(path), (err, dirent) => {
    if (err) { return cb(err); }
    // Create the inode for the new file (we need its _id for the directory)
    mf.db.inodes.insert(inode, (err, doc) => {
      if (err) { return cb(fuse.EIO); }
      // Create the new directory entry
      const newdir = {
        name:   pathmod.basename(path),
        parent: dirent._id,
        inode:  doc._id
      };
      mf.db.directory.insert(newdir, (err, doc) => {
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
  mf.DEBUG('Resolve path %s', path);

  // Split path into components
  // Note that splitting "/" on "/" gives two empty-string components, we don't want that
  if (path === "/") { path = ""; }
  const names = path.split("/");

  // Create a series of steps to look up each component of the path in turn
  const tasks = names.map((name /*:string*/) =>
    (pdoc, cb) => {
      // Look up one component of the path in the directory, return its entry
      // The root is represented with a null parent and an empty-string name
      mf.db.directory.findOne({ name: name, parent: pdoc._id }, (err, doc) => {
        if (err)  { return cb(fuse.EIO,    null); }
        if (!doc) { return cb(fuse.ENOENT, null); }
        cb(null, doc);
      });
    }
  );
  // The first task in a waterfall doesn't get any args apart from the callback
  tasks.unshift(cb => cb(null, { _id: null }));

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
  mf.DEBUG('Truncate inode %s to %d bytes', inode, size);

  // Look up that inode
  mf.db.inodes.findOne({ _id: inode }, (err, doc) => {
    if (err)  { return cb(fuse.EIO); }
    if (!doc) { return cb(fuse.ENOENT); }
    // Permissions?
    const access = mf.chkaccess(doc, 0x2);
    if (access) { return cb(access); }
    // Note MongoDB's max document size.
    // This leaves a bit of space for the rest of the inode data.
    if (size > 16000000) { return cb(fuse.EFBIG); }

    // Truncate to the requested size, by copying into a buffer of that size
    const buf = new Buffer(size).fill(0);
    if (doc.data) { doc.data.read(0, size).copy(buf); }
    // Update the inode
    const set = {
      ctime: Date.now(),
      mtime: Date.now(),
      data:  new mongojs.Binary(buf, 0)
    };

    mf.db.inodes.update({ _id: inode }, { $set: set }, (err, result) => {
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
  mf.DEBUG('Get attributes of inode %s', inode);

  // Look up that inode
  mf.db.inodes.findOne({ _id: inode }, { data: false }, (err, doc) => {
    if (err)  { return cb(fuse.EIO); }
    if (!doc) { return cb(fuse.ENOENT); }

    // Look up link count, required to support hardlinks
    mf.db.directory.count({ inode: inode }, (err, cnt) => {
      if (err) { return cb(fuse.EIO); }
      doc.nlink = cnt;

      // Find and store the size if we didn't already have it
      if (doc.size === undefined) {
        mf.DEBUG('Size not stored, finding it');
        mf.db.inodes.findOne({ _id: inode }, (err, idoc) => {
          if (err)   { return cb(fuse.EIO); }
          if (!idoc) { return cb(fuse.ENOENT); }
          doc.size = idoc.data ? idoc.data.length() : 0;

          mf.db.inodes.update({ _id: inode }, { $set: { size: doc.size } }, (err, iidoc) => {
            if (err) { return cb(fuse.EIO); }
            return cb(0, doc);
          });
        });

      // We already had it, return the inode as it now stands
      } else {
        return cb(0, doc);
      }
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
  mf.DEBUG('Remove inode %s if unreferenced', inode);

  // Is it open?
  for (let fd in mf.openFiles) {
    if (mf.openFiles[fd].inode === inode) { return cb(0); }
  }

  // Look up refcount
  mf.db.directory.count({ inode: inode }, (err, cnt) => {
    // Does it have any links?
    // note: is this a race condition (TOCTOU)?
    if (err) { return cb(fuse.EIO); }
    if (cnt) { return cb(0); }

    // If not, remove the inode
    mf.db.inodes.remove({ _id: inode }, true, (err, res) => {
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
  mf.DEBUG('Check if user %d in group %d', uid, gid);

  const pwnam = posix.getpwnam(uid);
  const grnam = posix.getgrnam(gid);
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
function chkaccess(inode /*:{_id:string,mode:number,uid:number,gid:number}*/, mode /*:number*/) {
  mf.DEBUG('Check access mode %d allowed to inode %s', mode, inode._id);

  const context = fuse.context();
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
    const ubits  = (inode.mode >> 6) & 7;
    const umatch = ((ubits & mode) === mode);
    return umatch ? 0 : fuse.EACCES;
  } else if (mf.useringroup(context.uid, inode.gid)) {
    // User requesting access is in this file's group
    const gbits  = (inode.mode >> 3) & 7;
    const gmatch = ((gbits & mode) === mode);
    return gmatch ? 0 : fuse.EACCES;
  } else {
    // Neither user nor group match, check world permissions
    const obits  = (inode.mode >> 0) & 7;
    const omatch = ((obits & mode) === mode);
    return omatch ? 0 : fuse.EACCES;
  }
}

/**
 * Check whether an inode object's atime needs updating, and if so, do it.
 *
 * @param {Object}   inode
 * @param {Function} cb
 */
function chkatime(inode /*:{_id:string,atime:number,ctime:number,mtime:number}*/, cb /*:function*/) {
  if (
    global.ATIME_LEVEL === 2 ||    // atime
    (global.ATIME_LEVEL === 1 && ( // relatime
      // Is atime older than mtime, older than ctime, or older than a day?
      // See: http://lxr.free-electrons.com/source/fs/inode.c#L1541
      inode.mtime >= inode.atime ||
      inode.ctime >= inode.atime ||
      (Date.now() - inode.atime) >= (24 * 60 * 60 * 1000)
    ))
  ) {
    // Update it
    mf.DEBUG('Updating atime for inode %s', inode._id);
    inode.atime = Date.now();
    mf.db.inodes.update({ _id: inode._id }, { $set: { atime: inode.atime } }, cb);
  } else {
    // Doesn't need updating
    cb(0);
  }
}

/**
 * Encode a string in Base64
 *
 * @param   {String} str
 * @returns {String}
 */
function b64enc(str /*:string*/) {
  const buf = new Buffer(str);
  return buf.toString('base64');
}

/**
 * Decode a string from Base64
 *
 * @param   {String} str
 * @returns {String}
 */
function b64dec(str /*:string*/) {
  const buf = new Buffer(str, 'base64');
  return buf.toString();
}
