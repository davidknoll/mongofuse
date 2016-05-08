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
      db.inodes.findOne({ _id: openFiles[fd].inode }, function (err, doc) {
        if (err)  { return cb(fuse.EIO); }
        if (!doc) { return cb(fuse.ENOENT); }
        // doc.data is a MongoDB "Binary" object. read()ing it gives a Node "Buffer" object.
        var srcbuf = doc.data.read(pos, len);
        var copied = srcbuf.copy(buf);
        cb(copied);
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
