"use strict";
const request = require('request');
const fs      = require('fs');
const path    = require('path');
const cheerio = require('cheerio');
const parse   = require('csv-parse');
const http    = require('http');
const proxy   = require('proxy');

//params
const proxy_list_path = 'proxy-list1.csv'

//retries
const retry_limit = 7
const retry_timeout = 1000

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

function createProxiedRequest(host, user, pass, port) {
  var proxyUrl = "http://" + user + ":" + pass + "@" + host + ":" + port;
  var proxiedRequest = request.defaults({'proxy': proxyUrl});
  //console.log(proxyUrl)
  return proxiedRequest
}

if (process.argv.length < 3) {
  throw new Error('No input title file specified. Usage: node index.js <titlefilepath>')
}

const titleFilePath = path.resolve('.', process.argv[2]);

//from my experience, any qps higher than 1 will result in your proxy request being blocked sometimes
const qps = process.argv[3] || 1;
const titles = fs.readFileSync(titleFilePath, 'utf8')
  .split(/\r|\n|\r\n/)
  .filter(title => title.trim().length > 0);

const numRequests = titles.length;
let completedRequests = 0;

var today = new Date();
var current_date = today.getFullYear() + '_' + (today.getMonth()+1) + '_' + today.getDate()
var myfilepath = 'scraped_data/google_jobs_results_' + current_date + '.tsv'

//output path for base job data
const outputFilePath = path.join(process.cwd(), myfilepath);
const outputFileStream = fs.createWriteStream(outputFilePath);
outputFileStream.write('Query\tTitle\tCompany\tLocation\tproxyLocation\tproxyHost\tProvider\tRecency\tJobType\tRank\tjobDescription\tjobLink\tCurrentTime\n');

//output path for run stats
const statsLogFilePath = path.join(process.cwd(), 'log/stats_' + current_date + '.tsv');
const statsLogFileStream = fs.createWriteStream(statsLogFilePath);

//output path for complete failures (failed even after retries)
const errorLogFilePath = path.join(process.cwd(), 'log/complete_failures_' + current_date + '.tsv');
const errorLogFileStream = fs.createWriteStream(errorLogFilePath);

const mytest = path.join(process.cwd(), 'test.txt');
const mytestStream = fs.createWriteStream(mytest);

String.prototype.replaceAll = function(search, replacement) {
    var target = this;
    return target.replace(new RegExp(search, 'g'), replacement);
};

/**
 *
 * @param {CheerioStatic} $ - Cheerio object
 */
function processResponse($, query, proxiedRequest, proxyLocation, proxyHost) {

  console.log("Length of the response is: " + String($('li._yQk').length));

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

    let today = new Date();
    let current_time = (today.getFullYear()) + ("0" + (today.getMonth()+1)).slice(-2) + ("0" + today.getDate()).slice(-2) + ("0" + (today.getHours())).slice(-2) + ("0" + today.getMinutes()).slice(-2) + ("0" + today.getSeconds()).slice(-2);

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
      current_time
    ];
    //after retriving the jobId, now you can deeplink into the specific job in order to get the job description
    return tsvColumns.join('\t');
  }).get().join('\n');
};

//Proxy location




/**
After processResponse() and getting the jobid, we want to execute another request to get the job description
**/
function makeRequest(title, proxiedRequest, proxy_indexes, proxyLocation, proxyHost, locationIndex, retryCount) {
  
  const job_query = title + ' ' + proxyLocation.toLowerCase();
  const url = `https://www.google.com/search?q=${job_query.replace(' ', '+')}&ibp=htl;jobs`;
  let d = new Date();

  proxiedRequest.get(url, {
    headers: {
      //'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36 OPR/38.0.2220.41'
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
    }
  }, (err, res, body) => {
    //console.log(`${url}`)
    if (res == undefined || res.statusCode === 503) {
      const errortitle = `${title}`
      const message = "Google has blocked request for: " + `${title}` + " and location: " + `${proxyLocation}` + ", retry count: " + String(retryCount + 1) 
      statsLogFileStream.write(String(d) + '\t' + title + '\t' + String(proxyHost) + '\t' + proxyLocation + '\t' + String(1) + '\t' + String(retryCount) + '\n');

      if (retryCount < retry_limit) {
        console.log(message);
        
        let host_index     = generateHostIndex(proxy_indexes, locationIndex);
        let proxy_info     = generateProxyInformation(host_index);
        let proxiedRequest = createProxiedRequest(proxy_info.host, proxy_info.user, proxy_info.pass, proxy_info.port);
        let proxyUrl = "http://" + proxy_info.user + ":" + proxy_info.pass + "@" + proxy_info.host + ":" + proxy_info.port;

        setTimeout(() => {makeRequest(title, proxiedRequest, proxy_indexes, proxyLocation, proxyHost, locationIndex, retryCount + 1)}, retry_timeout)
        return
      
      } else {
        console.log("Retry limit exceeded, moving on to next request")
        errorLogFileStream.write(errortitle + '\t' + proxyLocation + '\n');
        return
      }
    } else {
      console.log(`Request Suceeded for title: ${title} and location: ${proxyLocation}, Finished: ${++completedRequests}`);

      //console.log(body);

      const $ = cheerio.load(body);
      const results = processResponse($, title, proxiedRequest, proxyLocation, proxyHost);

      outputFileStream.write(results + '\n');
      statsLogFileStream.write(String(d) + '\t' + title + '\t' + String(proxyHost) + '\t' + proxyLocation + '\t' + String(0) + '\t' + String(0) + '\n');
      
      return
    }
  });
};


function getCurrentIp(proxiedRequest) {
  /*
  Get current IP address thats being executed to check that the proxy is working.
  */

  let url = "http://ipinfo.io";  
  proxiedRequest.get(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Linux; <Android Version>; <Build Tag etc.>) AppleWebKit/<WebKit Rev>(KHTML, like Gecko) Chrome/<Chrome Rev> Safari/<WebKit Rev>'
    }
  }, (err, res, body) => {
    if (res == undefined || res.statusCode === 503) {
      console.log(`Ip check failed`);
      return;
    }
    const $ = cheerio.load(body);
    console.log($('#heading').text());
  });
};

function generateHostIndex(proxy_indexes, index) {
  /*
  Generate a random index for the given proxy location that you want

  input: index from the proxy index list
  output: a random index for the proxy that you want.  
  */

  return Math.floor((proxy_indexes[index+1] - proxy_indexes[index])*Math.random() + proxy_indexes[index]);
}

function generateProxyInformation(host_index) {
  /*
  Takes as input an index from the proxy_list and generates information about that proxy.  
  */
  let host          = String(proxy_list[host_index][0]);
  let port          = String(proxy_list[host_index][1]);
  let user          = String(proxy_list[host_index][2]);
  let pass          = String(proxy_list[host_index][3]);
  let proxyLocation = String(proxy_list[host_index][4]);

  return {
    host:          host,
    port:          port,
    user:          user,
    pass:          pass,
    proxyLocation: proxyLocation
  };
};

//this formula scales depending on the length of the proxy list as well as how many groups you have within that proxy list.
function makeAllRequests(titles, scaling_factor, proxy_indexes) {
  //store error keywords in a list to retry later
  var errors = [];
  titles.forEach((title, i) => {
    if (i >= start && i < end) {
      if (i % qps === 0) {
        time += 1000 * scaling_factor;
      }
      for (let j = 0; j < proxy_indexes.length - 1; j++) {
        // cycle through proxies randomly
        let host_index    = generateHostIndex(proxy_indexes, j);
        let proxy_info    = generateProxyInformation(host_index);

        let proxiedRequest = createProxiedRequest(proxy_info.host, proxy_info.user, proxy_info.pass, proxy_info.port);
        let proxyUrl = "http://" + proxy_info.user + ":" + proxy_info.pass + "@" + proxy_info.host + ":" + proxy_info.port;

        //console.log(proxyUrl)
        //use the function below if you want to 
        //getCurrentIp(proxiedRequest);
        setTimeout(() => {
          //console.log(proxyUrl);
          //console.log("what is j: " + j)
          makeRequest(title, proxiedRequest, proxy_indexes, proxy_info.proxyLocation, proxy_info.host, j, 0)  //last param is retry count, set that to zero and increment if the request fails
        }
        , time * (.8 + Math.random()/2.5) //generate a number between .8 and 1.2
        ); 
      }
    };
  });
};

let time = 0; 
console.log(`Total Queries: ${numRequests}`);
let start = 0;
let end = titles.length;

setTimeout(function() {
  let proxy_indexes = [0,25,51,77,100, proxy_list.length];
  //let proxy_indexes = [104, proxy_list.length];
  //let proxy_indexes = [0, proxy_list.length]
  let scaling_factor = (proxy_indexes.length - 1) * (proxy_indexes.length - 1) * 60 / proxy_list.length / 2;
  console.log("proxy indexes: " + proxy_indexes)
  console.log("The estimated time for completion is: " + titles.length * scaling_factor/3600 + " hours.")
  makeAllRequests(titles, scaling_factor, proxy_indexes);
}, 100)





