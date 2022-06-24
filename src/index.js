
// import {GetObjectCommand, S3Client} from '@aws-sdk/client-s3'
// import { S3Client } from "@aws-sdk/client-s3";
// import { GetObjectCommand } from "@aws-sdk/client-s3";

// import {createRequire} from "module";

// const require = createRequire(import.meta.url);


// import S3 from 'aws-sdk/clients/s3';
// import {Readable} from 'stream';

const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");

// import {GetObjectCommand, S3Client} from '@aws-sdk/client-s3';
// const {GetObjectCommand, S3Client} = pkg;
const http = require('http');
// const AWS = require('@aws-sdk/client-s3');
// const getStream = require('get-stream');

const LineStream = require('byline').LineStream;
const zlib = require('zlib');
const stream = require('stream');
// const s3 = new S3();

const hostname = '127.0.0.1';
const port = 3000;

const s3Client = new S3Client({
    // apiVersion: '2006-03-01',
    region: 'eu-west-2'
})

/**
 * Test program to validate the logs
 * Run the program as : node src/test.js
 * Call the program as: curl localhost:3000
 * @type {Request<Transfer.CreateServerResponse, AWSError> | Request<OpsWorksCM.CreateServerResponse, AWSError>}
 */
const server = http.createServer((req, res) => {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'text/plain');

    console.log('Received request.');

    let bucket = "some-bucket";
    // let objKey = "AWSLogs/676563297163/elasticloadbalancing/eu-west-2/2022/03/10/676563297163_elasticloadbalancing_eu-west-2_app.mcauk-uksr-dev-alb.ec08a9036d3eba3e_20220310T0005Z_3.8.44.36_66inuwaz.log.gz";
    let objKey = "AWSLogs/676563297163/elasticloadbalancing/eu-west-2/2022/03/10/676563297163_elasticloadbalancing_eu-west-2_app.mcauk-uksr-dev-alb.ec08a9036d3eba3e_20220310T0255Z_18.135.44.213_j2y3bpv5.log.gz";

    s3LogsToCW(bucket, objKey);

    console.log('Sent response.');

    res.end('Hello, World!\n');
});


async function s3LogsToCW(bucket, key) {
    // Note: The Lambda function should be configured to filter for .log.gz files
    // (as part of the Event Source "suffix" setting).
    //
    // const response = await s3.getObject({
    //     Bucket: bucket,
    //     Key: key,
    // });

    // const streamToString = (stream) =>
    //     new Promise((resolve, reject) => {
    //         const chunks = [];
    //         stream.on("data", (chunk) => chunks.push(chunk));
    //         stream.on("error", reject);
    //         stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    //     });

    const response = await s3Client.send(new GetObjectCommand({
        Bucket: bucket,
        Key: key,
    }));

    // const stream = response.Body
    let gunzipStream = zlib.createGunzip();
    var lineStream = new LineStream();
    // var recordStream = new stream.Transform({objectMode: true})
    var recordStream = new stream.Transform({objectMode: true})
    recordStream._transform = function(line, encoding, done) {
        var logRecord = parse(line.toString());
        var serializedRecord = JSON.stringify(logRecord);
        this.push(serializedRecord);
        done();
    }

    response.Body.pipe(gunzipStream)
        .pipe(lineStream)
        .pipe(recordStream)
        .on('data', function(parsedEntry) {
        console.log(parsedEntry);
    });

    response.Body.on('error', function() {
        console.log(
            'Error getting object "' + key + '" from bucket "' + bucket + '".  ' +
            'Make sure they exist and your bucket is in the same region as this function.');
        // context.fail();
    });

    // return new Promise((resolve, reject) => {
    //     const chunks = []
    //     stream.on('data', chunk => chunks.push(chunk))
    //     stream.once('end', () => resolve(Buffer.concat(chunks)))
    //     stream.once('error', reject)
    // })

    // const bodyContents = await streamToString(response.Body);
    // let s3Stream = response.Body.pipe(response);
    // console.log("response.Body ---> ", typeof (s3Stream));
    // console.log("bodyContents ---> ", bodyContents);


    // let gunzipStream = zlib.createGunzip();
    // item.Body.pipe(response.Body).pipe(gunzipStream);

    // s3Stream
    //     .pipe(gunzipStream);

    // let s3Stream = (await getStream.buffer(response.Body));
    // let gunzipStream = zlib.createGunzip();
    // s3Stream
    //     .pipe(gunzipStream)
    //     .pipe(lineStream)
    //     .pipe(recordStream)
    //     .on('data', function(
    //     ) {
    //         console.log(parsedEntry);
    //     });


    // const bodyContents = (await getStream.buffer(response.Body)).toString();
    // console.log(bodyContents)

    //
    // const response = await s3Client
    //     .send(new GetObjectCommand({
    //         Key: key,
    //         Bucket: bucket,
    //     }))

    // let s3Stream = response.Body.pipe(response);


    // let response = await s3.getObject({Bucket: bucket, Key: key});
    // let s3Stream = response.Body.pipe(response);
    // let s3Stream = s3.getObject({Bucket: bucket, Key: key}).createReadStream();
    // let gunzipStream = zlib.createGunzip();
    // // Flow: S3 file stream -> Log Line stream -> Log Record stream -> CW
    // s3Stream
    //     .pipe(gunzipStream)
    //     .pipe(lineStream)
    //     .pipe(recordStream)
    //     .on('data', function(parsedEntry) {
    //         console.log(parsedEntry);
    //     });
    // s3Stream.on('error', function() {
    //     console.log(
    //         'Error getting object "' + key + '" from bucket "' + bucket + '".  ' +
    //         'Make sure they exist and your bucket is in the same region as this function.');
    //     context.fail();
    // });
}


// async function streamToString(stream: Readable): Promise<string> {
//     return await new Promise((resolve, reject) => {
//         const chunks: Uint8Array[] = [];
//         stream.on('data', (chunk) => chunks.push(chunk));
//         stream.on('error', reject);
//         stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
//     });
// }

function parse(line) {

    var parsed = {};
    var url = require('url');
    var request_labels =
        [
            'request_method',
            'request_uri',
            'request_http_version',
            'request_uri_scheme',
            'request_uri_host',
            'request_uri_port',
            'request_uri_path',
            'request_uri_query'
        ];
    //
    // Trailing newline? NOTHX
    //
    if (line.match(/\n$/)) {
        line = line.slice(0, line.length - 1);
    }
    [
        { 'type'                        : ' '   },
        { 'timestamp'                   : ' '   },
        { 'elb'                         : ' '   },
        { 'client'                      : ':'   },
        { 'client_port'                 : ' '   },
        { 'target'                      : ' '   },
        { 'request_processing_time'     : ' '   },
        { 'target_processing_time'      : ' '   },
        { 'response_processing_time'    : ' '   },
        { 'elb_status_code'             : ' '   },
        { 'target_status_code'          : ' '   },
        { 'received_bytes'              : ' '   },
        { 'sent_bytes'                  : ' "'  },
        { 'request'                     : '" "' },
        { 'user_agent'                  : '" '  },
        { 'ssl_cipher'                  : ' '   },
        { 'ssl_protocol'                : ' '   },
        { 'target_group_arn'            : ' '   }
    ].some(function (t) {
        var label = Object.keys(t)[0];
        let delimiter;
        delimiter = t[label]
        var m = line.match(delimiter);
        let field;

        if (m === null) {
            //
            // No match. Try to pick off the last element.
            //
            m = line.match(delimiter.slice(0, 1));
            if (m === null) {
                field = line;
            }
            else {
                let field;
                field = line.substr(0, m.index);
            }
            parsed[label] = field;
            return true;
        }
        field = line.substr(0, m.index);
        line = line.substr(m.index + delimiter.length);
        parsed[label] = field;
    });
    // target
    if(typeof parsed.target !== 'undefined' && parsed.target !== -1 && parsed.target !== '-') {
        parsed['target_port'] = parsed.target.split(":")[1];
        parsed['target'] = parsed.target.split(":")[0];
    } else {
        parsed['target_port'] = '-1';
    }
    // target status code
    if(typeof parsed.target_status_code === 'undefined' || parsed.target_status_code.toString() === "-") {
        parsed['target_status_code'] = '0';
    }
    // request
    if(typeof parsed.request !== 'undefined' && parsed.request !== '- - - ') {
        var i = 0;
        var method = parsed.request.split(" ")[0];

        var url = url.parse(parsed.request.split(" ")[1]);
        var http_version = parsed.request.split(" ")[2];
        parsed[request_labels[i]] = method;
        i++;
        parsed[request_labels[i]] = url.href;
        i++;
        parsed[request_labels[i]] = http_version;
        i++;
        parsed[request_labels[i]] = url.protocol;
        i++;
        parsed[request_labels[i]] = url.hostname;
        i++;
        parsed[request_labels[i]] = url.port;
        i++;
        parsed[request_labels[i]] = url.pathname;
        i++;
        parsed[request_labels[i]] = url.query;
    } else if(typeof parsed.request !== 'undefined') {
        request_labels.forEach(function(label) {
            parsed[label] = '-';
        });
    }

    return parsed;
};

server.listen(port, hostname, () => {
    console.log(`Server running at http://${hostname}:${port}/`);
});
