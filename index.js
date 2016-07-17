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
var yargs   = require('yargs');
var mf      = require('./extra.js');
var ops     = require('./fuseops.js');

// Parse command-line arguments
// $FlowFixMe: Testing require.main === module is in the Node docs!
if (require.main === module) {
  var argv = yargs
    .alias({
      help:    'h',
      options: 'o',
      verbose: 'v',
      version: 'V'
    })
    .count('verbose')
    .demand(2, 2, 'A MongoDB connection string and a mount point are required. No further options other than the above are accepted.')
    .describe({
      options: 'FUSE mount options, comma-separated. See also: http://blog.woralelandia.com/2012/07/16/fuse-mount-options/',
      verbose: 'Show debugging output (can be specified twice)'
    })
    .epilogue('See also: https://www.npmjs.com/package/mongofuse')
    .example('$0 mongofuse ~/mongo', "Mounts a mongofuse filesystem in the 'mongofuse' database on localhost, to the directory ~/mongo")
    .help()
    .nargs('options', 1)
    .strict()
    .string(['_', 'options'])
    .usage('Usage: $0 <connection-string> <mount-point> [options]')
    .version()
    .argv;

  global.VERBOSE_LEVEL = argv.verbose;
  if (Array.isArray(argv.options)) {
    argv.o = argv.options = argv.options.filter(function (opt) { return typeof opt === 'string'; }).join();
  }
  if (typeof argv.options === 'string') {
    argv.o = argv.options = argv.options.split(',');
  }

  var returncode = main(argv);
  if (returncode) process.exit(returncode);
}

/**
 * @param   {Object} argv
 * @returns {Number}
 */
function main(argv /*:{_:Array<string>}*/) /*:number*/ {
  mf.db = mongojs(argv._[0], [ "directory", "inodes" ]);
  var mountPath = argv._[1];
  if (Array.isArray(argv.options)) {
    // $FlowIssue https://github.com/facebook/flow/issues/1606
    ops.options = argv.options;
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
