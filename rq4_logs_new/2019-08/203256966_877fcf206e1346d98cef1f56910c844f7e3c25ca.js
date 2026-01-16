const menus = {
    main: `
 Useage: dictionary <options> [word] 
    -a, --anantonym   .... Displays anantonym(s) of [word]
    -d, --defenition  .... Displays definition(s) of [word]
    -e, --example ........ Displays example(s) of [word]
    -dr, --derivation .... Display derivation(s) of [word]
    -p, --partof ......... Displays part(s) of speech of [word]
    -pr, --pronunciation . Displays pronunciation of [word]
    -s, --synonum ........ Displays synonum(s) of [word]
    -v, --version ........ Displays dictionary-cli version
    `,
  }
  
  module.exports = (args) => {
    const subCmd = args._[0] === 'help'
      ? args._[1]
      : args._[0]
  
    console.log(menus[subCmd] || menus.main)
  }