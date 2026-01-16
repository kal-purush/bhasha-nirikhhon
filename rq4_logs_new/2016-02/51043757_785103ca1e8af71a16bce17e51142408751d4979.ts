class Comment {
  body: string;
  author: string;
  id: string;
  
  constructor (body: string, author: string, id: string) {
    this.body = body;
    this.author = author;
    this.id = id;
  }
  
  /**
   * Takes an unanswered comment and prepares it get reposted.
   * 
   * @name prepareComment
   * @return The comment to repost.
   */
  prepareForRepost () {
    let repost: string = '';
    let bodyParts = this.body.split('\n');
    
    // Add the quote syntax to each paragraph of the comment body.
    for (let i = 0; i < bodyParts.length; i++) {
      if (bodyParts[i].trim() !== '') {
        repost += '>' + bodyParts[i] + '\n';      
      }
    }
    
    // Add attribution to orginal author and disclaimer.
    repost += '\n*Originally asked by /u/' + this.author + ' in yesterday\'s Q&A thread.*\n***\n^(Please message /u/BBQLays with any questions or comments about this bot.)';
    
    return repost;
  }
}

export = Comment;