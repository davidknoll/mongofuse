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
var fuse    = require('fuse-bindings');
var mongojs = require('mongojs');

// $FlowFixMe: Testing require.main === module is in the Node docs
if (require.main === module) {
  var argv = process.argv.slice(1);
  var argc = argv.length;
  var envp = process.env;
  process.exit(main(argc, argv, envp));
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

  return 0;
}
