/**
 * Vertebrae Inc
 * @package Cassandra-ORM
 */
"use strict";

/**
 * Determines the tailing argument (callback)
 * @param {object} arguments - an arguments object
 * @example
 * //args = arguments[1, 2, callback, null]
 * getArgCount(args); //3
 * //args = arguments[1, 2, null, callback, null]
 * getArgCount(args); //4
 */
exports.getArgCount = (args) => {
    var argc = args.length;
    while (argc && !args[argc-1]) {
        argc -= 1;
    }
    return argc;
};

/**
 * Filters args by default to Boolean
 * @param {object} arguments - an arguments object
 * @param {mixed} filter - an item passed to Array.prototype.filter
 * @example
 * //args = arguments[1, 2, null, callback]
 * getFilteredArgs(args); //[1,2,callback]
 */
exports.getFilteredArgs = (args, filter) => {
    return (args.length === 1 ? [args[0]] : Array.apply(null, args)).filter(filter || Boolean);
};
