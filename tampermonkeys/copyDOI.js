// ==UserScript==
// @name     copyDOI
// @run-at document-end
// @description copy DOI in clipboard
// @namespace   http://tampermonkey.net/
// @author      flowerpower13

// @match  http://*/*
// @match  https://*/*

// @require https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js

// @version  1
// ==/UserScript==

function doiSubfix(doi){
   var prefixRegex = '[^ ]*doi.org\/'
   return doi.replace(new RegExp(prefixRegex), '')
}

$( document ).ready(function() {
  var bodyContent = $(document.body).html()
  var urlsRegex = 'href="[^ ]*doi\.org[^ ]*"';
  var urls = bodyContent.match(urlsRegex);

  if(urls!=null && urls.length > 0) {

    var doi = urls[0].substring(6, urls[0].length-1)
    doi = doiSubfix(doi)

    window.prompt("Press Ctrl+C, Enter", doi)
    return
  } else {
    bodyContent = $(document.body).text()
    bodyContent = bodyContent.replace(/(?:\r\n|\r|\n)/g, ' ');
    
    urlsRegex = '[^ ]*doi.org\/[^ ]*';
    urls = bodyContent.match(urlsRegex);
    
    var doi2 = doiSubfix(urls[0])
    window.prompt("Press Ctrl+C, Enter", doi2)
  }

});


