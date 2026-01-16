import { ObjectType } from '@nestjs/graphql';

import { CommitNtOptions } from '@/git/git.service';

@ObjectType()
export class GitCommitEvent {
  /** 提交分支全称 */
  private tree: string;

  /** 提交属性 */
  private options: CommitNtOptions;

  constructor(tree: string, options: CommitNtOptions) {
    this.tree = tree;
    this.options = options;
  }

  getTree() {
    return this.tree;
  }

  getUser() {
    return this.options.committer;
  }

  getTables() {
    return this.options.tables;
  }

  getOptions() {
    return this.options;
  }

  // 查看修改包含特定表
  includeTableName(tableName: string) {
    return this.options.tables.includes(tableName);
  }
}