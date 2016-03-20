/*global document */
(function() {
    var source = document.getElementsByClassName('prettyprint source linenums');
    var i = 0;
    var lineNumber = 0;
    var lineId;
    var lines;
    var totalLines;
    var anchorHash;

    if (source && source[0]) {
        anchorHash = document.location.hash.substring(1);
        lines = source[0].getElementsByTagName('li');
        totalLines = lines.length;

        for (; i < totalLines; i++) {
            lineNumber++;
            lineId = 'line' + lineNumber;
            lines[i].id = lineId;
            if (lineId === anchorHash) {
                lines[i].className += ' selected';
            }
        }
    }
})();


//custom Vertebrae hijacking
(function () {
    var scripts = document.documentElement.getElementsByTagName('script');
    var script = document.createElement('script');
    script.async = true;
    script.src = 'scripts/vertebrae.js';
    scripts[0].insertBefore(script, scripts[0].parent);
}());
