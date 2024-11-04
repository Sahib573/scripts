const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs');

const data = {};
const incorrectData = {};

async function scrapeWikipediaPage(url) {
    try {
        const headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        };
        const response = await axios.get(url, { headers });
        if (response.status !== 200) {
            throw new Error(`Error fetching the page ${url}`);
        }
        const $ = cheerio.load(response.data);

        const tableRows = $('table.wikitable tbody tr');
       

        tableRows.each((index, row) => {
            const cells = $(row).find('td');
            if (cells.length > 2) {
                const unNumber = $(cells[0]).text().trim();  
                const classInfo = $(cells[1]).text().trim(); 
                const shippingName = $(cells[2]).text().trim();

                // Skip if shippingName contains 'UN No. no longer in use'
                if (shippingName.includes('UN No. no longer in use')) {
                    incorrectData[unNumber] = { class: classInfo, shipping_name: shippingName };
                } else {
                    if (classInfo !== '?' && classInfo !== '-') {
                        // Regex to extract class and division, ignoring any trailing letter
                        const classMatch = classInfo.match(/^(\d+)(\.\d+)?[A-Z]?$/);
                        if (classMatch) {
                            const dangerous_good_class = classMatch[1];
                            const dangerous_good_division = classMatch[2] ? classMatch[1] + classMatch[2] : ''; 

                            data[unNumber] = {
                                class: dangerous_good_class,
                                division: dangerous_good_division,
                                shipping_name: shippingName
                            };
                        } else {
                            incorrectData[unNumber] = { class: classInfo, shipping_name: shippingName };
                        }
                    } else {
                        incorrectData[unNumber] = { class: classInfo, shipping_name: shippingName };
                    }
                }
            }
        });

        console.log(`Data successfully saved to JS file from: ${url}`);
    } catch (error) {
        console.error(`Error scraping data from ${url}:`, error);
    }
}

async function writeToJSFile(data, filePath) {
    const content = `module.exports = ${JSON.stringify(data, null, 2)};`;
    fs.writeFileSync(filePath, content, 'utf8');
    console.log(`Data written to ${filePath}`);
}

async function getUNNumberUrls() {
    try {
        const headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        };

        // Fetch the Wikipedia page that lists all the UN number lists
        const response = await axios.get('https://en.wikipedia.org/wiki/Lists_of_UN_numbers', { headers });
        if (response.status !== 200) {
            throw new Error('Error fetching the main UN number lists page');
        }

        const $ = cheerio.load(response.data);

        // Find all 'ul' elements whose parent 'div' has the class 'mw-heading mw-heading2'
        const links = [];
        $('div.mw-heading2').next('ul').find('li a').each((index, element) => {
            const relativeUrl = $(element).attr('href');
            const fullUrl = `https://en.wikipedia.org${relativeUrl}`;
            links.push(fullUrl);
        });

        return links;
    } catch (error) {
        console.error('Error fetching UN number URLs:', error);
        return [];
    }
}

async function scrapeAllUNPages() {
    const urls = await getUNNumberUrls();
    for (const url of urls) {
        console.log(`Scraping: ${url}`);
        await scrapeWikipediaPage(url);
    }
    await writeToJSFile(data, 'un_numbers_masterdata.js');
    await writeToJSFile(incorrectData, 'un_numbers_incorrect.js');
}
scrapeAllUNPages();
