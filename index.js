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
  var mountPath = argv[2];
  fuse.mount(mountPath, {

    readdir: function (path, cb) {
      console.log('readdir(%s)', path);
      if (path === '/') return cb(0, ['test']);
      cb(0);
    },

    getattr: function (path, cb) {
      console.log('getattr(%s)', path);
      if (path === '/') {
        cb(0, {
          mtime: new Date(),
          atime: new Date(),
          ctime: new Date(),
          size: 100,
          mode: 16877,
          uid: process.getuid ? process.getuid() : 0,
          gid: process.getgid ? process.getgid() : 0
        });
        return;
      }

      if (path === '/test') {
        cb(0, {
          mtime: new Date(),
          atime: new Date(),
          ctime: new Date(),
          size: 12,
          mode: 33188,
          uid: process.getuid ? process.getuid() : 0,
          gid: process.getgid ? process.getgid() : 0
        });
        return;
      }

      cb(fuse.ENOENT);
    },

    open: function (path, flags, cb) {
      console.log('open(%s, %d)', path, flags);
      cb(0, 42); // 42 is an fd
    },

    read: function (path, fd, buf, len, pos, cb) {
      console.log('read(%s, %d, %d, %d)', path, fd, len, pos);
      var str = 'hello world\n'.slice(pos);
      if (!str) return cb(0);
      buf.write(str);
      return cb(str.length);
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
