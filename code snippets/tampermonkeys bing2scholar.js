// ==UserScript==
// @name        bing2scholar
// @run-at document-start
// @description Redirect URL from Bing to Google Scholar
// @namespace   http://tampermonkey.net/
// @author      flowerpower13

// @match     http://*.bing.com/*
// @match     https://*.bing.com/*

// @version     1
// @grant       GM_openInTab
// ==/UserScript==

var url1='https://scholar.google.com/scholar?hl=en&as_sdt=0,5&lr=lang_en&'+document.URL.match(/q\=[^&]*/)+'"';
var url2=url1.replace('lang_en&q=','lang_en&q=allintitle%3A+"')
var url3="https://webz.telegram.org/"

if (document.URL!=url2) {
    location.replace(url2);
//    GM_openInTab (url2);
}
