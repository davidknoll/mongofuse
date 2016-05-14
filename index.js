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
  if (argc !== 3) {
    console.log("Usage: " + argv[0] + " connection-string mountpoint");
    return 1;
  }

  mf.db = mongojs(argv[1], [ "directory", "inodes" ]);
  var mountPath = argv[2];

  fuse.mount(mountPath, ops, function (err) {
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
