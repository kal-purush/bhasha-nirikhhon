const vowel=/[a,e,i,o,u]/g;
/*  
    STEP # 1: Encode words that begin with a vowel sound from english to pig latin
    For words that begin with vowel sounds, one just adds "yay" to the end.

    For example:
        "eat" becomes "eatyay"
        "omelet" becomes "omeletyay" 
*/
  function encodeVowelWord(word) {
    var i=word.toLowerCase().search(vowel);
    if (i==0)
       return (word+"yay")
    return word; // replace this!
  }
  
  /*  
      STEP # 2: Encode words that begin with a consonant sound from english to pig latin.
      For words that begin with consonant sounds, all letters before the initial vowel 
      are placed at the end of the word sequence, preceded by a hyphen "-". Then, "ay" is added. 
      
      For example:
          "latin" becomes "atin-lay"
          "cheers" becomes "eers-chay"
  */
  function encodeConsonantWord(word) {
    var latinWord='';
    var i=1;
    if (word.toLowerCase().search(vowel)>-1)
            i=word.toLowerCase().search(vowel);
    latinWord=word.slice(i,word.length)+"-"+word.substring(0, i)+"ay";
    return latinWord; // replace this!
  }
  
  /*  
      STEP # 3: Decide whether a given word starts with a vowel sound or consonant sound,
      and call encodeVowelWord(word) or encodeConsonantWord(word) when relevant.
  
      For example:
          "eat" becomes "eatyay" because it starts with a vowel "e"
          "omelet" becomes "omeletyay" because it starts with a vowel "o"
          "latin" becomes "atin-lay" because it starts with a consonant "l""
          "cheers" becomes "eers-chay" because it starts with a consonant cluster "ch"
          "you" becomes "ou-yay" because it starts with a consonant "y"
  */
  function encodeWord(word) {
    if (word.toLowerCase().search(vowel)==0)
        return encodeVowelWord(word)
    return encodeConsonantWord(word) ; // replace this!
  }
  
  /*
      STEP # 4: Encode a full sentence or paragraph from english to pig latin.
  */
  function encodeText(text) {
    console.log(text);  
    var pigLatinText="";  
    var words=text.split(" ");
    for(var word = 0; word < words.length; word++){
      words[word]=encodeWord(words[word]);
    }  
    pigLatinText=words.join(" ");
   // document.getElementById('encodedMsg').value=pigLatinText;
    //console.log();
    return pigLatinText; // replace this!
  }
  
  /*
      STEP # 5: Create a web form where users can input any message in plain english
      and get it encoded into pig latin.
  */
  
  /*  
      STEP # 6: Decode pig latin words into words that begin with a vowel sound. 
      In other words, do the reverse of encodeVowelWord.
  
      For example:
          "eatyay" becomes "eat"
          "omeletyay" becomes "omelet" 
  */
  function decodeVowelWord(word) {
    var i=word.toLowerCase().search(vowel);
    if (i==0)
       return (word.slice(0,word.lastIndexOf("yay")))
    return word; // replace this!
  }
  
  /*  
      STEP # 7: Decode pig latin words into words that begin with a consonant sound. 
      In other words, do the reverse of encodeConsonantWord.
      
      For example:
          "atin-lay" becomes "latin"
          "eers-chay" becomes "cheers"
  */
  function decodeConsonantWord(word) {
    var decodedWord="";
    var words=word.split("-")
    var lastWord=words[(words.length)-1];
    var i=lastWord.toLowerCase().search(vowel);
    if (i>-1)
       lastWord=lastWord.slice(0,i);
    decodedWord=lastWord;
    for (i=0;i<(words.length-1);i++)
      decodedWord+=words[i];
    return decodedWord; // replace this!
  }
  
  /*  
      STEP # 8: Decide whether a given word starts with a vowel sound or consonant sound,
      and call decodeVowelWord(word) or decodeConsonantWord(word) when relevant.
      In other words, do the reverse of encodeWord.
  
      For example:
          "eatyay" becomes "eat" because it ends with "yay"
          "omeletyay" becomes "omelet" because it ends with "yay"
          "atin-lay" becomes "latin" because it ends with a hyphen, a consonant sound, and an "ay"
          "eers-chay" becomes "cheers" because it ends with a hyphen, a consonant sound, and an "ay"
          "ou-yay" becomes "you" because it ends with a hyphen, a consonant sound, and an "ay"
  */
  function decodeWord(word) {
    if (word.includes("-"))
       return decodeConsonantWord(word);

    return decodeVowelWord(word); // replace this!
  }
  
  /*
      STEP # 9: Decode a full sentence or paragraph pig latin to english.
  */
  function decodeText(text) {
    var decodedText="";  
    var words=text.split(" ");
    for(var word = 0; word < words.length; word++){
      words[word]=decodeWord(words[word]);
    }  
    decodedText=words.join(" ");
    return decodedText; // replace this!
  }
  
  /*
      STEP # 10: Create a web form where users can input any message in pig latin and get it 
      decoded into plain english.
  */
  
  /*
      BONUS: Go back to encodeText and decodeText and modify it so it can gracefully handle punctuation 
      such as '.', ',', '?'
  */
  
  /* ===============
     Below are some helper variables and functions that are already programmed to help check your progress. YOU DO NOT NEED TO CHANGE ANYTHING BELOW THIS LINE. 
     =============== */
  
  const testVowelWords = {
    eat: 'eatyay',
    omelet: 'omeletyay',
    are: 'areyay',
    egg: 'eggyay',
    explain: 'explainyay',
    always: 'alwaysyay',
    ends: 'endsyay',
    every: 'everyyay',
    another: 'anotheryay',
    under: 'underyay',
    island: 'islandyay',
    elegant: 'elegantyay',
  };
  
  const testSimpleConsonantWords = {
    latin: 'atin-lay',
    banana: 'anana-bay',
    trash: 'ash-tray',
    happy: 'appy-hay',
    duck: 'uck-day',
    dopest: 'opest-day',
    me: 'e-may',
    too: 'oo-tay',
    will: 'ill-way',
    moist: 'oist-may',
    wet: 'et-way',
    real: 'eal-ray',
    simple: 'imple-say',
    say: 'ay-say',
    bagel: 'agel-bay',
    you: 'ou-yay',
  };
  
  const testClusteredConsonantWords = {
    cheers: 'eers-chay',
    shesh: 'esh-shay',
    smile: 'ile-smay',
    string: 'ing-stray',
    thanks: 'anks-thay',
    stupid: 'upid-stay',
    glove: 'ove-glay',
  };
  
  const testWords = Object.assign(
    {},
    testVowelWords,
    testSimpleConsonantWords,
    testClusteredConsonantWords
  );
  
  const chipotleMessage =
    "ince-say e-thay irst-fay ipotle-chay openedyay inyay 1993, we've earned-lay ayay ot-lay aboutyay igs-pay. e-way ow-knay itsyay importantyay or-fay em-thay o-tay oot-ray, oam-ray, andyay expressyay eir-thay atural-nay endencies-tay, o-say e-way ork-way ith-way armers-fay o-whay aise-ray igs-pay at-thay ang-hay outyay outdoorsyay oryay inyay eeply-day edded-bay arns-bay. ey're-thay alsoyay ever-nay iven-gay aily-day oses-day ofyay antibioticsyay o-tay ake-may em-thay ow-gray aster-fay. ut-bay, eaking-spay ofyay allyay ings-thay ig-pay, at-whay onyay earthyay isyay ig-pay atin-lay? eally-ray, o-whay inventedyay ityay? en-whay o-day ou-yay useyay ityay? isyay ityay onlyyay or-fay eens-tway? oes-day ityay ake-may ou-yay ound-say art-smay? isyay ityay onlyyay usedyay y-bay armers-fay? isyay is-thay ust-jay ayay ong-lay etup-say or-fay ayay ogwash-hay oke-jay? areyay ou-yay ill-stay eading-ray? o-say any-may uestions-qay, o-say ittle-lay ime-tay. en-thay againyay, ifyay ou're-yay itting-say ere-thay ith-way ayay ag-bay ull-fay ofyay urritos-bay, aybe-may ou-yay ave-hay e-thay ime-tay o-tay onder-pay andyay ecode-day allyay is-thay. andyay ifyay at's-thay e-thay ase-cay, en-thay itsyay ack-bay, unwrapyay, andyay onder-pay awayyay.";
  
  const simpleChipotleMessage = chipotleMessage
    .replace(/,/g, '')
    .replace(/\./g, '')
    .replace(/\?/g, '');
  
  /* ==========
     Unit Tests
     ========== */
  
  function testEncodeVowelWords(words) {
    for (var key in words) {
      let result = encodeVowelWord(key);
      console.assert(result === words[key], {
        message: 'english to pig latin failed',
        word: key,
        expected: words[key],
        result: result,
      });
    }
  }
  
  function testEncodeConsonantWords(words) {
    for (var key in words) {
      let result = encodeConsonantWord(key);
      console.assert(result === words[key], {
        message: 'english to pig latin failed',
        word: key,
        expected: words[key],
        result: result,
      });
    }
  }
  
  function testEncodeWords(words) {
    for (var key in words) {
      let result = encodeWord(key);
      console.assert(result === words[key], {
        message: 'english to pig latin failed',
        word: key,
        expected: words[key],
        result: result,
      });
    }
  }
  
  function testEncodeText(words) {
    const keys = Object.keys(words);
    const values = Object.values(words);
    const text = keys.join(' ');
    let result = encodeText(text);
    let expected = values.join(' ');
    if (result != expected) {
      console.assert(result === expected, {
        message: 'english to pig latin failed',
        text: text,
        expected: expected,
        result: result,
      });
    }
  }
  
  function testDecodeVowelWords(words) {
    for (var key in words) {
      let result = decodeVowelWord(words[key]);
      console.assert(result === key, {
        message: 'pig latin to english failed',
        word: words[key],
        expected: key,
        result: result,
      });
    }
  }
  
  function testDecodeConsonantWords(words) {
    for (var key in words) {
      let result = decodeConsonantWord(words[key]);
      console.assert(result === key, {
        message: 'pig latin to english failed',
        word: words[key],
        expected: key,
        result: result,
      });
    }
  }
  
  function testDecodeWords(words) {
    for (var key in words) {
      let result = decodeWord(words[key]);
      console.assert(result === key, {
        message: 'pig latin to english failed',
        word: words[key],
        expected: key,
        result: result,
      });
    }
  }
  
  function testDecodeText(words) {
    const keys = Object.keys(words);
    const values = Object.values(words);
    const text = values.join(' ');
    let result = decodeText(text);
    let expected = keys.join(' ');
    if (result != expected) {
      console.assert(result === expected, {
        message: 'pig latin to english failed',
        text: text,
        expected: expected,
        result: result,
      });
    }
  }
  
  console.log(
    'Testing STEP # 1: encoding words that begin with a vowel sound...'
  );
  if (encodeVowelWord('apple') != '') {
    testEncodeVowelWords(testVowelWords);
  } else {
    console.log('encodeVowelWord not implemented!');
  }
  console.log(
    'Testing STEP # 2: encoding words that begin with a consonant sound...'
  );
  if (encodeConsonantWord('test') != '') {
    testEncodeConsonantWords(testSimpleConsonantWords);
  } else {
    console.log('encodeConsonantWord not implemented!');
  }
  console.log(
    'Testing STEP # 2 (cont): encoding words that begin with a consonant clusters...'
  );
  if (encodeConsonantWord('test') != '') {
    testEncodeConsonantWords(testClusteredConsonantWords);
  } else {
    console.log('encodeConsonantWord not implemented!');
  }
  console.log(
    'Testing STEP # 3: deciding to encode between vowel and consonant words...'
  );
  if (encodeWord('test') != '') {
    testEncodeWords(testWords);
  } else {
    console.log('encodeWord not implemented!');
  }
  console.log('Testing STEP # 4: encode a sentence (no punctuation)...');
  if (encodeText('test') != '') {
    testEncodeText(testWords);
  } else {
    console.log('encodeText not implemented!');
  }
  console.log(
    'Testing STEP # 6: decoding words that begin with a vowel sound...'
  );
  if (decodeVowelWord('appleyay') != '') {
    testDecodeVowelWords(testVowelWords);
  } else {
    console.log('decodeVowelWord not implemented!');
  }
  console.log(
    'Testing STEP # 7: decoding words that begin with a consonant sound...'
  );
  if (decodeConsonantWord('est-tay') != '') {
    testDecodeConsonantWords(testSimpleConsonantWords);
  } else {
    console.log('decodeConsonantWord not implemented!');
  }
  console.log(
    'Testing STEP # 7 (cont): decoding words that begin with a consonant clusters...'
  );
  if (decodeConsonantWord('est-tay') != '') {
    testDecodeConsonantWords(testClusteredConsonantWords);
  } else {
    console.log('decodeConsonantWord not implemented!');
  }
  console.log(
    'Testing STEP # 8: deciding to decode between vowel and consonant words...'
  );
  if (decodeWord('appleyay') != '') {
    testDecodeWords(testWords);
  } else {
    console.log('decodeWord not implemented!');
  }
  console.log('Testing STEP # 9: encode a sentence (no punctuation)...');
  if (decodeText('appleyay') != '') {
    testDecodeText(testWords);
  } else {
    console.log('decodeText not implemented!');
  }
  console.log('FINAL: Decoding the chipotle message (no punctuation)...');
  if (decodeText('appleyay') != '') {
    console.log(decodeText(simpleChipotleMessage));
  } else {
    console.log('decodeText not implemented!');
  }
  console.log('BONUS: Decoding the chipotle message (with punctuation)...');
  if (decodeText('appleyay') != '') {
    console.log(decodeText(chipotleMessage));
  } else {
    console.log('decodeText not implemented!');
  }
  