import tri from 'tripartite';
import {test1, test2} from "../views/load-browser-views.js"

export default function go() {
	console.log(test2())

	// and use like:
	let d = document.createElement('div')
	d.innerHTML = tri.getTemplate('test1')({
		key1: 'value'
		, key2: 'value'
	})
	document.body.append(d)
}

export function stop() {
	console.log('stop')
}