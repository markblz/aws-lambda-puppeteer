const chromium = require('@sparticuz/chromium');
const puppeteer = require('puppeteer-core');

const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');

const dynamoClient = new DynamoDBClient({ region: 'sa-east-1' });

// Function to remove accents and special characters from text.
function removeSpecialCharacters(text) {
  if (typeof text !== 'string') {
    return text;
  }

  const nfkdForm = text.normalize('NFD');
  const withoutAccent = nfkdForm.replace(/[\u0300-\u036f]/g, '');

  return withoutAccent;
}

// Function to sanitize JSON data by removing accents and special characters from keys and values.
function sanitizeJson(data) {
  if (typeof data === 'object' && data !== null) {
    if (Array.isArray(data)) {
      return data.map(sanitizeJson);
    } else {
      return Object.keys(data).reduce((acc, key) => {
        acc[removeSpecialCharacters(key)] = sanitizeJson(data[key]);
        return acc;
      }, {});
    }
  } else if (typeof data === 'string') {
    return removeSpecialCharacters(data);
  }
  return data;
}

async function launchBrowser() {
  try {
    console.log("Attempting to launch the browser...");

    const browser = await puppeteer.launch({
      args: chromium.args,
      defaultViewport: chromium.defaultViewport,
      executablePath: await chromium.executablePath(),
      headless: true,
      timeout: 60000,
    });

    console.log("Browser launched successfully");
    return browser;
  } catch (err) {
    console.error("Error launching the browser:", err);
    throw err;
  }
}

exports.lambdaHandler = async (event, context) => {
  let browser;

  try {
    console.log("Launching headless browser...");
    browser = await launchBrowser();
    console.log("Browser launched successfully");

    const page = await browser.newPage();
    console.log("New page opened");

    // Function to handle responses
    async function handleResponse(response) {
      if (response.url().includes("publicacao/pesquisa/")) {
        console.log("Received a JSON response");

        try {
          if (response.headers()['content-type'] === 'application/json') {
            const jsonData = await response.json();
            console.log("JSON extracted successfully");

            // Sanitize the JSON data
            const sanitizedData = sanitizeJson(jsonData);
            console.log("JSON sanitized");

            // Process each item in the 'collection' array
            if (Array.isArray(sanitizedData.collection)) {
              for (const item of sanitizedData.collection) {
                const uniqueId = item.numeroPublicacao;
                if (!uniqueId) {
                  console.log("Unique identifier 'numeroPublicacao' not found in item. Skipping.");
                  continue;
                }

                // Add item to DynamoDB with conditional write
                const putItemParams = {
                  TableName: 'Publications',
                  Item: {
                    'numeroPublicacao': { N: uniqueId.toString() },
                    'dataPublicacao': { S: item.dataPublicacao },
                    'decisao': { S: JSON.stringify(item.decisao) },
                    'textoPublicacao': { S: item.textoPublicacao },
                    'fontePublicacao': { S: JSON.stringify(item.fontePublicacao) },
                    'id': { N: item.id.toString() },

                    // Add additional fields with null checks
                    'tribunalSigla': item.decisao?.usuario?.instancia?.tribunal?.sigla ? 
                                    { S: item.decisao.usuario.instancia.tribunal.sigla } : 
                                    { NULL: true },
                    
                    'tipoDecisao': item.decisao?.tipoDecisao?.nome ? 
                                  { S: item.decisao.tipoDecisao.nome } : 
                                  { NULL: true },
                    
                    'partes': {
                      L: item.decisao?.partes?.map(parte => ({
                        M: {
                          'nomeParte': parte.nome ? 
                                      { S: parte.nome } : 
                                      { NULL: true },
                          
                          'advogados': {
                            L: parte.advogados?.map(adv => ({
                              M: {
                                'nomeAdvogado': adv.nome ? 
                                                { S: adv.nome } : 
                                                { NULL: true },
                                
                                'numeroOAB': adv.numero ? 
                                            { S: adv.numero } : 
                                            { NULL: true },
                                
                                'ufSigla': adv.uf?.sigla ? 
                                          { S: adv.uf.sigla } : 
                                          { NULL: true }
                              }
                            })) || [] // Return empty list if no advogados
                          }
                        }
                      })) || [] // Return empty list if no partes
                    }
                  },
                  ConditionExpression: 'attribute_not_exists(numeroPublicacao)',
                };
                try {
                  await dynamoClient.send(new PutItemCommand(putItemParams));
                  console.log(`Added new item with numeroPublicacao: ${uniqueId} to DynamoDB.`);
                } catch (err) {
                  if (err.name === 'ConditionalCheckFailedException') {
                    console.log(`Item with numeroPublicacao ${uniqueId} already exists. Skipping.`);
                  } else {
                    console.error(`Error inserting item into DynamoDB for numeroPublicacao ${uniqueId}:`, err);
                  }
                }
              }
            } else {
              console.log("No 'collection' array found in JSON data.");
            }
          }
        } catch (err) {
          console.error('Error handling JSON response:', err);
        }
      }
    }

    page.on('response', handleResponse);

    console.log("Starting navigation...");
    // Navigate to the target webpage
    await page.goto('https://mural-consulta.tse.jus.br/mural/dashboard', {
      waitUntil: 'networkidle2',
    });
    console.log("Page loaded");

    // Click the initial button
    const initialButtonSelector = 'body > app-root > div > app-base > mat-sidenav-container > mat-sidenav-content > div > lib-dashboard-publicacao > div > mat-grid-list > div > mat-grid-tile:nth-child(4) > figure > div > button';

    await page.waitForSelector(initialButtonSelector, { timeout: 10000, visible: true });
    console.log("Initial button found");
    await page.click(initialButtonSelector);
    console.log("Initial button clicked");

    // Wait for navigation or content to load
    try {
      await page.waitForNavigation({ waitUntil: 'networkidle0', timeout: 20000 });
      console.log("Navigation after initial button click completed");
    } catch (err) {
      console.log("Navigation timeout or error: ", err);
    }

    // Optional: Take a screenshot for debugging
    // Note: In Lambda, you may need to handle the screenshot differently
    // const screenshot = await page.screenshot({ encoding: 'base64', fullPage: true });
    // console.log(`Screenshot after clicking initial button: data:image/png;base64,${screenshot}`);

    // Wait for the form that contains the buttons
    const formSelector = 'body > app-root > div > app-base > mat-sidenav-container > mat-sidenav-content > div lib-list-publicacao form';
    await page.waitForSelector(formSelector, { timeout: 20000 });
    console.log("Form loaded");

    // Get all the buttons within the form
    const buttons = await page.$$(formSelector + ' button');

    console.log(`Found ${buttons.length} buttons to click.`);

    for (const [index, button] of buttons.entries()) {
      try {
        console.log(`Clicking button ${index + 1}...`);
        await button.click();
        console.log(`Clicked button ${index + 1}`);
        // Wait for any dynamic content to load after clicking each button
        await new Promise(resolve => setTimeout(resolve, 4000));
      } catch (err) {
        console.log(`Error clicking button ${index + 1}: `, err);
      }
    }

    console.log("Data collection and storage in DynamoDB completed.");

    return {
      statusCode: 200,
      body: JSON.stringify({ message: "Data collection and storage in DynamoDB completed." }),
    };
  } catch (error) {
    console.error('Error in scraping process:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Scraping process failed', message: error.message }),
    };
  } finally {
    if (browser) {
      await browser.close();
      console.log("Browser closed");
    }
  }
};