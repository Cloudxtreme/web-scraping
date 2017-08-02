const request = require('request');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

if (process.argv.length < 3) {
  throw new Error('No input title file specified. Usage: node index.js <titlefilepath>')
}

const titleFilePath = path.resolve('.', process.argv[2]);
const qps = process.argv[3] || 1;

const titles = fs.readFileSync(titleFilePath, 'utf8')
  .split(/\r|\n|\r\n/)
  .filter(title => title.trim().length > 0);

const numRequests = titles.length;

let completedRequests = 0;


const outputFilePath = path.join(process.cwd(), 'google_jobs_results_mobile.tsv');
const outputFileStream = fs.createWriteStream(outputFilePath);

outputFileStream.write('Query\tTitle\tCompany\tLocation\tProvider\tRecency\tJobType\tRank\n');

/**
 *
 * @param {CheerioStatic} $ - Cheerio object
 */
function processResponse($, query) {
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
    }

    const rank = i + 1;
    const tsvColumns = [
      query,
      title,
      company,
      location,
      provider,
      recency,
      jobType,
      rank
    ]

    return tsvColumns.join('\t');
  }).get().join('\n');
}


function makeRequest(title) {
  const url = `https://www.google.com/search?q=${title.replace(' ', '+')}&ibp=htl;jobs`;

  request(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19'
    }
  }, (err, res, body) => {
    console.log(`Finished: ${++completedRequests}`);
    if (res.statusCode === 503) {
      console.log(`Google blocked request for query: ${title}`);
      return;
    }
    const $ = cheerio.load(body);
    const results = processResponse($, title);
    outputFileStream.write(results + '\n');

    if (completedRequests === numRequests) {
      outputFileStream.end();
    }
  });
}



let time = 0;

console.log(`Total Queries: ${numRequests}`);

titles.forEach((title, i) => {
  if (i % qps === 0) {
    time += 1000;
  }

  setTimeout(() => makeRequest(title), time);
});
