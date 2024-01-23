// DON'T USE THIS
// This package now assumes that wigets will be built as modules.
// If, on consideration, you decide they should be built common js style, this is
// the starting point.
let tri = require('tripartite')


// load templates like
let test1 = require('../views/test1.tri')
let test2 = require('../views/test2.tri')


console.log(test2())

// and use like:
let d = document.createElement('div')
d.innerHTML = tri.getTemplate('views/test1')({
	key1: 'value'
	, key2: 'value'
})
document.body.append(d)





