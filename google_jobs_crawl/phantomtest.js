
var http = require("http");
var phantom = require("phantom");
const fs      = require('fs');
const path    = require('path');

var myfilepath = 'test.txt'

//output path for base job data
const outputFilePath = path.join(process.cwd(), myfilepath);
const outputFileStream = fs.createWriteStream(outputFilePath);
outputFileStream.write('Query\tTitle\tCompany\tLocation\tproxyLocation\tproxyHost\tProvider\tRecency\tJobType\tRank\tjobDescription\tjobLink\tCurrentTime\n');

phantom.create(function (ph) {
  ph.createPage(function (page) {
    var url = "https://www.google.com/search?q=software+engineer+jobs&ibp=htl;jobs";

    page.open(url, function() {
      page.includeJs("http://ajax.googleapis.com/ajax/libs/jquery/1.6.1/jquery.min.js", function() {
        page.evaluate(function() {

          $('li._yQk').each(function (i, el) {
            const title = $el.find('._grr').text();
            outputFileStream.write(title);
            outputFileStream.write("hello")
            console.log("TESTING123")

            });

          });
        }, function(){
          ph.exit()
        });
      });
    });
  });
