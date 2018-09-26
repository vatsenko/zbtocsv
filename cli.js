#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const _ = require('lodash');
const csv = require('csvtojson');
const jsonexport = require('jsonexport');
const async = require('async');
const chalk = require('chalk');

const [, , ...args] = process.argv;

const log = console.log;
const error = chalk.bold.red;
const info = chalk.green;
const msg = chalk.yellow;
const msg1 = chalk.cyan;


let getJSON = function(filename, callback) {
    const fn = filename.replace(/.csv/, '');
    if (fs.existsSync(ouputFolder + '/' + fn + '.json')) {
        log(info('Reading ' + fn + '.json'));
        return callback(null, JSON.parse(fs.readFileSync(ouputFolder + '/' + fn + '.json')));
    }

    log(msg('Parsing as JSON ' + filename));

    let keyIndex = filename.replace(/.csv/, '') + '_PK';
    let res = {};
    csv()
        .fromFile(path.join(projectFolder + '/' + filename))
        .then((j) => {
            j.forEach(el => { res[el[keyIndex]] = el; });
            fs.writeFileSync(ouputFolder + '/' + fn + '.json', JSON.stringify(res, null, '\t'), { encoding: 'utf8' });
            return callback(null, res);

        });
};

let getRelates = function(filename, callback) {
    const fn = filename.replace(/.csv/, '').replace(/#rel/i, '').replace(/##/g, '.');

    if (fs.existsSync(ouputFolder + '/' + fn + '.json')) {
        log(info('Reading ' + fn + '.json'));
        return callback(null, JSON.parse(fs.readFileSync(ouputFolder + '/' + fn + '.json')));
    }

    log(msg1('Parsing as Array ' + filename));

    const label = fn.split('.')[0] + '_PK';
    let res = {};
    csv({ output: "json" })
        .fromFile(path.join(projectFolder + '/' + filename))
        .then((j) => {
            let labels = Object.keys(j[0]);
            let val = labels.splice(labels.indexOf(label, 1))[0];

            if (val.indexOf('_PK') > 0) {
                const relOrigin = val.replace(/_PK/, '');
                getJSON(relOrigin + '.csv', (err, origin) => {
                    if (err) { return log(err('err:' + elOrigin + '.csv, ' + err)); }
                    j.forEach(el => {
                        try {
                            if (!res[el[label]]) {
                                res[el[label]] = origin[el[val]].ID;
                            } else {
                                temp = res[el[label]];
                                res[el[label]] = temp + ',' + origin[el[val]].ID;
                            }
                        } catch (error) { log(error('error in ' + filename + ', el:' + JSON.stringify(el) + ', ' + el[label] + ', ' + val)); }
                    });
                    fs.writeFileSync(ouputFolder + '/' + fn + '.json', JSON.stringify(res, null, '\t'), { encoding: 'utf8' });
                    return callback(null, res);
                });
            } else {
                j.forEach(el => { res[el[label]] = el[val]; });
                fs.writeFileSync(ouputFolder + '/' + fn + '.json', JSON.stringify(res, null, '\t'), { encoding: 'utf8' });
                return callback(null, res);
            }
        });
};


//=====================================================================================
//
//

var projectFolder = args[1];

if (!projectFolder) {
    projectFolder = './';
} else if (!projectFolder.startsWith('/')) {
    projectFolder = './' + projectFolder;
}

const tableName = args[0];

let ouputFolder = args[2];

if (!ouputFolder) {
    ouputFolder = './' + tableName;
} else if (!ouputFolder.startsWith('/')) {
    ouputFolder = './' + ouputFolder;
}

if (tableName) {

    if (!fs.existsSync(ouputFolder)) {
        fs.mkdirSync(ouputFolder, 0744);
    }
    let files = fs.readdirSync(projectFolder);
    getJSON(tableName + '.csv', (err, res) => {

        let relates_files = _.filter(files, f => { return f.indexOf(tableName + '##') == 0; });
        let tasks = [];

        relates_files.forEach(rel => {
            tasks.push((callback) => {
                const name = rel.replace(/.csv/, '').replace(/#rel/i, '').replace(/##/g, '.').replace(tableName + '.', '');
                getRelates(rel, (err, data) => {
                    obj = {};
                    obj[name] = data;
                    return callback(null, obj);
                })
            })
        });

        async.parallel(tasks, (err, relates) => {
            if (err) { log(err('err:' + elOrigin + '.csv, ' + err)); }

            relates.forEach(rel => {
                log(info('Processing lookup ' + Object.keys(rel)[0]));
            })
            _.forEach(res, (val, key) => {
                relates.forEach(rel => {
                    const prop = Object.keys(rel)[0];
                    res[key][prop] = rel[prop][key] || '';
                    delete res[key][tableName + '_PK'];
                });
            });
            //fs.writeFileSync(ouputFolder + '/_' + tableName + '.json', JSON.stringify(res, null, '\t'), { encoding: 'utf8' });
            const output = _.values(res);
            log(msg('Saving to _' + tableName + ' ... '));
            fs.writeFileSync(ouputFolder + '/_' + tableName + '.json', JSON.stringify(output, null, '\t'), { encoding: 'utf8' });
            jsonexport(output, { rowDelimiter: ',', textDelimiter: '"', forceTextDelimiter: true }, (err, csv) => {
                if (err) { log(err(err)); }
                fs.writeFileSync(ouputFolder + '/_' + tableName + '.csv', csv, { encoding: 'utf8' });
                log(('DONE.'));
            })

        })
    });
}