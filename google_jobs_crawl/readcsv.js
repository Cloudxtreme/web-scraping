//example of how to read csv file

var fs = require('fs'); 
var parse = require('csv-parse');

var proxy_list=[];

fs.createReadStream('proxy-list.csv')
    .pipe(parse({delimiter: ','}))
    .on('data', function(csvrow) {
        proxy_list.push(csvrow);
               
    })
    .on('end',function() { 
      //console.log(csvData);
    });

setTimeout(function() {proxy_list.splice(0,1); console.log(proxy_list[0])}, 10)
