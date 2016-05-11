#!/usr/bin/node
/**
 * A FUSE filesystem, backed by MongoDB and written in Node.js.
 * Mostly an exercise for me to learn MongoDB and Node.js.
 *
 * @author  David Knoll <david@davidknoll.me.uk>
 * @license MIT
 * @file
 * @flow
 */

// Imports
var async   = require('async');
var daemon  = require('daemon');
var fuse    = require('fuse-bindings');
var mongojs = require('mongojs');

// C-isms for commandline args
// $FlowFixMe: Testing require.main === module is in the Node docs!
if (require.main === module) {
  var argv = process.argv.slice(1);
  var argc = argv.length;
  var envp = process.env;
  var returncode = main(argc, argv, envp);
  if (returncode) process.exit(returncode);
}

/**
 * @param   {Number} argc
 * @param   {Array}  argv
 * @returns {Number}
 */
function main(argc /*:number*/, argv /*:Array<string>*/) /*:number*/ {
  if (argc !== 3) {
    console.log("Usage: " + argv[0] + " connection-string mountpoint");
    return 1;
  }

  //daemon();
  var db = mongojs(argv[1], [ "directory", "inodes" ]);
  var mountPath = argv[2];
  var openFiles = {
    next: 10,
    add: function (data) {
      var fd = this.next++;
      this[fd] = data;
      return fd;
    }
  };

  // Takes a canonical path, relative to the root of our fs but with leading /,
  // runs callback with its directory entry
  function resolvePath(path, cb) {
    // Split path into components
    // Note that splitting "/" on "/" gives two empty-string components, we don't want that
    if (path === "/") { path = ""; }
    var names = path.split("/");

    // Create a series of steps to look up each component of the path in turn
    var tasks = names.map(function (name) {
      return function (pdoc, cb) {
        // Look up one component of the path in the directory, return its entry
        // The root is represented with a null parent and an empty-string name
        db.directory.findOne({ name: name, parent: pdoc._id }, function (err, doc) {
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

  // Truncates the file with the specified inode ID to the specified size
  function itruncate(inode, size, cb) {
    // Look up that inode
    db.inodes.findOne({ _id: inode }, function (err, doc) {
      if (err)  { return cb(fuse.EIO); }
      if (!doc) { return cb(fuse.ENOENT); }
      // Truncate to the requested size, by copying into a buffer of that size
      var buf = new Buffer(size).fill(0);
      if (doc.data) { doc.data.read(0, size).copy(buf); }
      // Update the inode
      var set = {
        ctime: Date.now(),
        mtime: Date.now(),
        data:  new mongojs.Binary(buf, 0)
      };
      db.inodes.update({ _id: inode }, { $set: set }, function (err, result) {
        if (err || !result.ok || !result.n) { return cb(fuse.EIO); }
        cb(0);
      });
    });
  }

  fuse.mount(mountPath, {

    readdir: function (path, cb) {
      // Look up the requested directory itself
      resolvePath(path, function (err, dirent) {
        if (err) { return cb(err); }
        // Look up children of the requested directory
        db.directory.find({ parent: dirent._id }, function (err, docs) {
          if (err) { return cb(fuse.EIO); }
          var names = docs.map(function (cdir) { return cdir.name; });
          // According to POSIX we're only meant to return . and .. if the entries actually exist,
          // but if we don't they won't appear in a directory listing
          names.unshift('..');
          names.unshift('.');
          cb(0, names);
        });
      });
    },

    getattr: function (path, cb) {
      // Look up the requested directory entry
      resolvePath(path, function (err, dirent) {
        if (err) { return cb(err); }
        // And look up the inode it refers to
        db.inodes.findOne({ _id: dirent.inode }, function (err, doc) {
          if (err)  { return cb(fuse.EIO); }
          if (!doc) { return cb(fuse.ENOENT); }
          // Get this live rather than storing it
          if (doc.data) { doc.size = doc.data.length(); }
          cb(0, doc);
        });
      });
    },

    open: function (path, flags, cb) {
      // This is only good for files that exist right now
      resolvePath(path, function (err, dirent) {
        if (err) { return cb(err); }
        var fd = openFiles.add({
          inode: dirent.inode,
          flags: flags
        });
        cb(0, fd);
      });
    },

    read: function (path, fd, buf, len, pos, cb) {
      // Look up the inode of the open file
      db.inodes.findOne({ _id: openFiles[fd].inode }, function (err, doc) {
        if (err)  { return cb(fuse.EIO); }
        if (!doc) { return cb(fuse.ENOENT); }
        if (!doc.data) { return cb(0); }
        // doc.data is a MongoDB "Binary" object. read()ing it gives a Node "Buffer" object.
        var srcbuf = doc.data.read(pos, len);
        var copied = srcbuf.copy(buf);
        cb(copied);
      });
    },

    write: function (path, fd, buf, len, pos, cb) {
      // Look up the inode of the open file
      db.inodes.findOne({ _id: openFiles[fd].inode }, function (err, doc) {
        if (err)  { return cb(fuse.EIO); }
        if (!doc) { return cb(fuse.ENOENT); }
        // Make sure we have a buffer of exactly the size of the write data
        var dstbuf = new Buffer(len).fill(0);
        var copied = buf.copy(dstbuf, 0, 0, len);
        if (!doc.data) { doc.data = new mongojs.Binary(new Buffer(0), 0); }
        doc.data.write(dstbuf, pos);
        // Update the inode (yes we're storing the data with the inode right now)
        var set = {
          ctime: Date.now(),
          mtime: Date.now(),
          data:  doc.data
        };
        db.inodes.update({ _id: openFiles[fd].inode }, { $set: set }, function (err, result) {
          if (err || !result.ok || !result.n) { return cb(fuse.EIO); }
          cb(copied);
        });
      });
    },

    chmod: function (path, mode, cb) {
      // Look up the requested directory entry
      resolvePath(path, function (err, dirent) {
        if (err) { return cb(err); }
        // And look up the inode it refers to
        db.inodes.findOne({ _id: dirent.inode }, function (err, doc) {
          if (err)  { return cb(fuse.EIO); }
          if (!doc) { return cb(fuse.ENOENT); }
          // In testing, the mode passed in does have the file type bits set,
          // but we probably don't want to go changing them so mask them just in case
          var set = {
            ctime: Date.now(),
            mode:  (doc.mode & 0170000) | (mode & 07777)
          };
          db.inodes.update({ _id: dirent.inode }, { $set: set }, function (err, result) {
            if (err || !result.ok || !result.n) { return cb(fuse.EIO); }
            cb(0);
          });
        });
      });
    },

    chown: function (path, uid, gid, cb) {
      // Look up the requested directory entry
      resolvePath(path, function (err, dirent) {
        if (err) { return cb(err); }
        // And look up the inode it refers to
        db.inodes.findOne({ _id: dirent.inode }, function (err, doc) {
          if (err)  { return cb(fuse.EIO); }
          if (!doc) { return cb(fuse.ENOENT); }
          // If we're setting only uid or only gid, the other will be -1
          var set = { ctime: Date.now() };
          if (uid >= 0) { set.uid = uid; }
          if (gid >= 0) { set.gid = gid; }
          db.inodes.update({ _id: dirent.inode }, { $set: set }, function (err, result) {
            if (err || !result.ok || !result.n) { return cb(fuse.EIO); }
            cb(0);
          });
        });
      });
    },

    utimens: function (path, atime, mtime, cb) {
      // Look up the requested directory entry
      resolvePath(path, function (err, dirent) {
        if (err) { return cb(err); }
        // And look up the inode it refers to
        db.inodes.findOne({ _id: dirent.inode }, function (err, doc) {
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
          db.inodes.update({ _id: dirent.inode }, { $set: set }, function (err, result) {
            if (err || !result.ok || !result.n) { return cb(fuse.EIO); }
            cb(0);
          });
        });
      });
    },

    truncate: function (path, size, cb) {
      // Truncating the inode by path
      resolvePath(path, function (err, dirent) {
        if (err) { return cb(err); }
        itruncate(dirent.inode, size, cb);
      });
    },

    ftruncate: function (path, fd, size, cb) {
      // Truncating the inode by open file descriptor
      itruncate(openFiles[fd].inode, size, cb);
    },

    mknod: function (path, mode, dev, cb) {
      // mode includes file type bits, dev is (major << 8) + minor
      // (and is called rdev in the inode / what gets returned by getattr)
      var pathmod = require('path');
      resolvePath(pathmod.dirname(path), function (err, dirent) {
        if (err) { return cb(err); }
        var context = fuse.context();
        var newinode = {
          mode:  mode,
          uid:   context.uid,
          gid:   context.gid,
          rdev:  dev,
          ctime: Date.now()
        };
        db.inodes.insert(newinode, function (err, doc) {
          if (err) { return cb(fuse.EIO); }
          var newdir = {
            name:   pathmod.basename(path),
            parent: dirent._id,
            inode:  doc._id
          };
          db.directory.insert(newdir, function (err, doc) {
            if (err) { return cb(fuse.EIO); }
            cb(0);
          });
        });
      });
    }

  }, function (err) {
    if (err) throw err;
    console.log('filesystem mounted on ' + mountPath);
  });

  process.on('SIGINT', function () {
    fuse.unmount(mountPath, function () {
      console.log('filesystem at ' + mountPath + ' unmounted');
      process.exit();
    });
  });

  return 0;
}
