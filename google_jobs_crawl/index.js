"use strict";

const request = require('request');
const fs      = require('fs');
const path    = require('path');
const cheerio = require('cheerio');
const parse   = require('csv-parse');

//proxy
const http    = require('http');
const proxy   = require('proxy');

//params
const proxy_list_path = 'proxy-list1.csv'

//get csv list
const proxy_list=[];
fs.createReadStream(proxy_list_path)
    .pipe(parse({delimiter: ','}))
    .on('data', function(csvrow) {
        proxy_list.push(csvrow);        
    })
    .on('end',function() {
        //remove header from proxy list
        proxy_list.splice(0,1); 
    });

//split proxy_list into group, need to write a function to generate this list automatically later
const proxy_indexes = [0,26,52,78,104, proxy_list.length]

//const host = '198.98.111.3'
//var user = 'huan'
//var pass = '50USa19' 
//var port = '8080'

function createProxiedRequest(host, user, pass, port) {
  var proxyUrl = "http://" + user + ":" + pass + "@" + host + ":" + port;
  var proxiedRequest = request.defaults({'proxy': proxyUrl});
  console.log(proxyUrl)
  return proxiedRequest
}

if (process.argv.length < 3) {
  throw new Error('No input title file specified. Usage: node index.js <titlefilepath>')
}
const titleFilePath = path.resolve('.', process.argv[2]);

//from my experience, any qps higher than 1 will result in 
const qps = process.argv[3] || 1;
const titles = fs.readFileSync(titleFilePath, 'utf8')
  .split(/\r|\n|\r\n/)
  .filter(title => title.trim().length > 0);

const numRequests = titles.length;
let completedRequests = 0;

var today = new Date();
var current_date = today.getFullYear()+'_'+(today.getMonth()+1)+'_'+today.getDate();
var myfilepath = 'scraped_data/google_jobs_results_' + current_date + '.tsv'

//output path for base job data
const outputFilePath = path.join(process.cwd(), myfilepath);
const outputFileStream = fs.createWriteStream(outputFilePath);
outputFileStream.write('Query\tTitle\tCompany\tLocation\tproxyLocation\tproxyHost\tProvider\tRecency\tJobType\tRank\tjobDescription\tjobLink\tCurrentDate\n');

//output path for base job data
const errorLogFilePath = path.join(process.cwd(), 'log/scraping_errors.txt');
const errorLogFileStream = fs.createWriteStream(errorLogFilePath);

String.prototype.replaceAll = function(search, replacement) {
    var target = this;
    return target.replace(new RegExp(search, 'g'), replacement);
};

/**
 *
 * @param {CheerioStatic} $ - Cheerio object
 */
function processResponse($, query, proxiedRequest, proxyLocation, proxyHost) {
  return $('li._yQk').map((i, el) => {
    const $el = $(el);
    const title = $el.find('._grr').text();

    const $textFields = $el.find('._Ebt');
    const company = $textFields.eq(0).text();
    const location = $textFields.eq(1).text();
    const provider = $textFields.eq(2).text().replace('via', '').trim();

    const $metaFields = $el.find('._TJq:not(._AMk)');
    let recency = $metaFields.eq(0).text();
    let jobType = $metaFields.eq(1).text();
    if ($metaFields.length === 2) {
      if (recency.indexOf('ago') !== -1) {
        jobType = undefined;
      } else {
        recency = undefined;
      }
    };

    const rank = i + 1;
    const today = new Date();
    const current_date = today.getFullYear()+'-'+(today.getMonth()+1)+'-'+today.getDate();
    
    //get deep links for additional job descriptions
    const $jobId = $el.find('._PEs');
    const jobId = $jobId.eq(0).children().attr('id');

    //get the job description
    const $jobDescription_1 = $el.find('._zMk');
    const jobDescription_1 = $jobDescription_1.eq(0).text();

    const $jobDescription_2 = $el.find('._t1q');
    const jobDescription_2 = $jobDescription_2.eq(0).text();

    //remove tab so it doesn't affect the delimiter
    const jobDescription = (jobDescription_1 + jobDescription_2).replaceAll('\t',' ').replaceAll('\n','    ').replace(/[^a-z0-9 ,.?!]/ig, '');

    //get the job link
    const $jobLink = $el.find('._ano');
    const jobLink = $jobLink.children().eq(0).attr('href');
    //console.log(jobLink);

    const tsvColumns = [
      query,
      title,
      company,
      location,
      proxyLocation,
      proxyHost,
      provider,
      recency,
      jobType,
      rank,
      jobDescription,
      jobLink,
      current_date
    ];
    //after retriving the jobId, now you can deeplink into the specific job in order to get the job description
    return tsvColumns.join('\t');
  }).get().join('\n');
};

/**
After processResponse() and getting the jobid, we want to execute another request to get the job description
**/
function makeRequest(title, proxiedRequest, proxyLocation, proxyHost) {
  const url = `https://www.google.com/search?q=${title.replace(' ', '+')}&ibp=htl;jobs`;

  proxiedRequest.get(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.104 Safari/537.36'
    }
  }, (err, res, body) => {
    console.log(`${url}`)
    console.log(`Finished: ${++completedRequests}`);
    if (res == undefined || res.statusCode === 503) {
      const message = `Google blocked request for query: ${title}`
      console.log(message);
      errorLogFileStream.write(message + '\n');
      return;
    }

    const $ = cheerio.load(body);
    const results = processResponse($, title, proxiedRequest, proxyLocation, proxyHost);
    outputFileStream.write(results + '\n');

    if (completedRequests === numRequests) {
      outputFileStream.end();
      errorLogFileStream.end();
    }
  });
}

//get current ip address thats being executed
function getCurrentIp(proxiedRequest) {
  let url = "http://ipinfo.io";  
  proxiedRequest.get(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.104 Safari/537.36'
    }
  }, (err, res, body) => {
    if (res == undefined || res.statusCode === 503) {
      console.log(`Ip check failed`);
      return;
    }
    const $ = cheerio.load(body);
    //console.log($('#heading').text());
  });
};

let time = 0; 
console.log(`Total Queries: ${numRequests}`);

let start = 0;
let end = titles.length;
//let end = 20;

setTimeout(function() {

  titles.forEach((title, i) => {
    if (i >= start && i < end) {
      if (i % qps === 0) {
        time += 1000 * proxy_indexes.length * 1.5;
      }

      for (i = 0; i < proxy_indexes.length - 1; i++) {
        // cycle through proxies randomly
        //let host_index = Math.floor(proxy_list.length*Math.random());

        let host_index = Math.floor((proxy_indexes[i+1] - proxy_indexes[i])*Math.random() + proxy_indexes[i]);
        // cycle through proxies 
        //console.log(host_index)
        //console.log(proxy_list.length)

        let host          = String(proxy_list[host_index][0]);
        let port          = String(proxy_list[host_index][1]);
        let user          = String(proxy_list[host_index][2]);
        let pass          = String(proxy_list[host_index][3]);
        let proxyLocation = String(proxy_list[host_index][4]);

        let proxiedRequest = createProxiedRequest(host, user, pass, port);
        var proxyUrl = "http://" + user + ":" + pass + "@" + host + ":" + port;
        //use the function below if you want to 
        //getCurrentIp(proxiedRequest);
        setTimeout(() => makeRequest(title, proxiedRequest, proxyLocation, host), time);
      }
    };
  });

}, 100)






