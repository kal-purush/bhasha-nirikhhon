'use strict';

let MD = require('../entities/md');
let Tag = require('../entities/tag');
let Relation = require('../values/relation');

let findTagsByMDId = (md_id) => {
  return Tag.findByIds(
    Relation.findByMDId(md_id).map((self) => {
      return self.tag_id;
    })
  );
};

let findMDsByTagId = (tag_id) => {
  return MD.findByIds(
    Relation.findByTagId(tag_id).map((self) => {
      return self.md_id;
    })
  );
};

module.exports = {
  findTagsByMDId: findTagsByMDId,
  findMDsByTagId: findMDsByTagId
};