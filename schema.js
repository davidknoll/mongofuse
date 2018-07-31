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

// Imports
const mongoose = require('mongoose');
const ObjectId = mongoose.Schema.Types.ObjectId;

const Directory = mongoose.model('Directory', {
  name: String,
  parent: { type: ObjectId, ref: 'Directory' },
  inode: { type: ObjectId, ref: 'Inode' },
}, 'directory');

const Inode = mongoose.model('Inode', {
  mode: Number,
  uid: Number,
  gid: Number,
  rdev: Number,
  ctime: Number,
  mtime: Number,
  atime: Number,
  size: Number,
  data: Buffer,
  xattr: { type: Map, of: Buffer },
}, 'inodes');

// Exports
module.exports = {
  Directory,
  Inode,
};
