/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 */
"use strict";

exports.getArgCount = (args) => {
    var argc = args.length;
    while (argc && !args[argc-1]) {
        argc -= 1;
    }
    return argc;
};
