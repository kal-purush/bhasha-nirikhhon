/// <reference path="./refs"/>

import natural = require("natural");
import types = require("node-author-intrusion");

export function process(args: types.AnalysisArguments) {
  // Splitting is a pretty simple iterative process. We always have to break
  // apart using the chosen word tokenizer. At the moment, we only support a
  // single tokenizer for the text.
  var tokenizer = new natural.TreebankWordTokenizer();

  for (var lineIndex in args.content.lines) {
    // When we tokenize the line, we need to also build up a location for the
    // token (including length). This is place inside the line's tokens.
    var line = args.content.lines[lineIndex];
    splitTokens(args.content, line, tokenizer);
  }
}

function splitTokens(content: types.Content, line: types.Line, tokenizer) {
  // Split out the tokens using the tokenizer.
  var lineText = line.text;
  var lineLocation = line.location;
  var texts = tokenizer.tokenize(line.text);

  // Create the tokens with the text and location.
  var index = 0;

  for (var textIndex in texts) {
    // Pull out the token and figure where it is in the source file.
    var text = texts[textIndex];
    var lineIndex = lineText.indexOf(text, index);

    if (lineIndex < 0) {
      throw RangeError("Cannot find text of " + text + " in " + lineText);
    }

    index = lineIndex + text.length;

    // Create a token object for this token with its location.
    var location = new types.Location(
      lineLocation.path,
      lineLocation.beginLine,
      lineIndex,
      lineLocation.endLine,
      (lineIndex + text.length));
    var token = new types.Token(location, text);
    token.index = content.tokens.length;

    // Add the token to both the line and the content file. The content list is
    // used when we want to do operations across all paragraphs.
    line.tokens.push(token);
    content.tokens.push(token);
  }
}