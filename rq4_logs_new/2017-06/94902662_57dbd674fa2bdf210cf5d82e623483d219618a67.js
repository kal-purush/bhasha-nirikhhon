const jsonTranform = require('./lib')
const data = [
  {
    title : "title1",
    description: "description1",
    blog: "This is a blog.",
    date: "11/4/2013",
    extra : {
	link : {
	  linka : 'http://google.com'
	}
    },
    list1:[
	{
	    name:"mike"
	}
    ],
    list2:[
	{
	    item: "thing"
	}
    ],
    clearMe: "text"
  },
  {
    title : "title2",
    description: "description1",
    blog: "This is a blog.",
    date: "11/4/2013",
    extra : {
	link : "http://goo.cm"
    },
    list1:[
	{
	    name:"mike"
	}
    ],
    list2:[
	{
	    item: "thing"
	}
    ],
    clearMe: "text"
  }
]


const format = {
  Title: 'title',
  Link: 'extra.link',
  Hello: 'blog',
}

console.log(jsonTranform(data, format))