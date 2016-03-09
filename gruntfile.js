const path = require('path');
module.exports = (grunt) => {
    require('time-grunt')(grunt);
    require('jit-grunt')(grunt);
    var config = {
            pkg: grunt.file.readJSON('package.json'),
            shell: {
                buildDocs: 'rm -rf docs && ./node_modules/.bin/jsdoc -c jsdoc.json',
                lcov: {
                    options: {
                        stdout: true
                    },
                    command: 'rm -rf ./coverage; ./node_modules/.bin/istanbul cover ./node_modules/mocha/bin/_mocha --report lcovonly -- -R spec --recursive --require test/global.js'
                },
                report: {
                    options: {
                        stdout: true
                    },
                    command: 'npm run report'
                }
            },
            jshint: {
                build: {
                    options: {
                        jshintrc: path.join(__dirname, '.jshintrc')
                    },
                    files: {
                        src: [
                            '*.js',
                            'lib/*.js'
                        ]
                    }
                }
            },
            watch: {
                build: {
                    //glob filepaths, avoid glob watching directories as in "/**/*.js"
                    files: '<%= jshint.build.files.src %>',
                    tasks: ['jshint:build'],
                    options: {
                        spawn: false
                    }
                },
                docs: {
                    //glob filepaths, avoid glob watching directories as in "/**/*.js"
                    files: '<%= jshint.build.files.src %>',
                    tasks: ['shell:buildDocs'],
                    options: {
                        spawn: false
                    }
                }
            }
        };

    grunt.initConfig(config);
    // Load the plugin that provides the "uglify" task.
    grunt.loadNpmTasks('grunt-shell');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-shell');

    grunt.registerTask('default', ['watch:build']);
    grunt.registerTask('docs', ['shell:buildDocs']);
};
