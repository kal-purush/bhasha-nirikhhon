lib.socialmenu = HMENU
lib.socialmenu {
  special = directory
  special.value = 10
  entryLevel = 1
    wrap = <ul>|</ul>
  1 = TMENU
  1 {
    expAll = 1
    noBlur = 1
    NO {
      allWrap = |
      wrapItemAndSub = <li>|</li>
      doNotLinkIt = 1
      stdWrap.cObject = COA
      stdWrap.cObject {
        10 = TEXT
        10 {
          typolink {
            returnLast = url
            parameter.data = field:url
          }
          wrap = <a href="|">
        }
        21=FILES
        21 {      
          references {
            table = pages
            fieldName = media
          }
          renderObj = IMAGE
          renderObj {
            file {
              width = 450c
              height = 450c
              import.data = file:current:publicUrl
            }   
          }     
        }
        30 = TEXT
        30 {
          value = </a>
        }
      }
    }
  }
}