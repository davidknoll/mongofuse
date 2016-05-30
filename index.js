#!/usr/bin/env node
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
var fuse    = require('fuse-bindings');
var mongojs = require('mongojs');
var mf      = require('./extra.js');
var ops     = require('./fuseops.js');

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
  if ((argc !== 3) && (argc !== 5 || argv[3] !== "-o")) {
    console.log("Usage: " + argv[0] + " connection-string mountpoint [-o option[,option]...]");
    return 1;
  }

  mf.db = mongojs(argv[1], [ "directory", "inodes" ]);
  var mountPath = argv[2];
  if (argv[3] === "-o") {
    ops.options = argv[4].split(",");
    console.log("Mounting with options: " + ops.options.join(", "));
  }

  fuse.mount(mountPath, ops, function (err) {
    if (err) { throw err; }
    console.log("Filesystem mounted at: " + mountPath);
  });

  process.on('SIGINT', function () {
    fuse.unmount(mountPath, function () {
      console.log("Unmounted");
      process.exit();
    });
  });

  return 0;
}
