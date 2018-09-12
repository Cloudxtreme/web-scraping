const phantom = require('phantom');

(async function() {

    const instance = await phantom.create();
    const page = await instance.createPage();
    const status = await page.open('http://www.google.com/');
    console.log(status);
    const loadjquery = await status.includeJs("http://ajax.googleapis.com/ajax/libs/jquery/1.6.1/jquery.min.js");
    const content = await loadjquery.property('content');
    console.log(content);
    await instance.exit();

}());