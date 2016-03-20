/*global document */
(function() {
    var headerList = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6'];
    var i = 0;
    var headers;
    var c;
    var el;
    for ( ; i < headerList.length; i++) {
        c = 0;
        headers = document.getElementsByTagName(headerList[i]);
        for ( ; c < headers.length; c++) {
            el = headers[c];
            el.id = 'user-content-' 
                + el.innerText.toLowerCase()
                    .replace(/ /g, '-')
                    .replace(/,/g, '');
        }
    }
})();
