const chromium = require('@sparticuz/chromium');
const puppeteer = require('puppeteer-core'); // Use puppeteer-core instead of puppeteer, puppeteer is too big for AWS Lambda

async function launchBrowser() {
  
    const browser = await puppeteer.launch({
      args: chromium.args,
      defaultViewport: chromium.defaultViewport,
      executablePath: '/usr/bin/chromium',//await chromium.executablePath(),
      headless: true,//chromium.headless,
      timeout: 60000,
    });
    return browser;
  }

async function testBrowser() {
    let browser;
    try {
      console.log("Tentando lançar o navegador...");
      browser = await launchBrowser();
      const page = await browser.newPage();
      await page.goto('https://example.com', { waitUntil: 'networkidle2' });
      console.log("Página carregada com sucesso");
    } catch (error) {
      console.error("Erro ao lançar o navegador:", error);
    } finally {
      if (browser) {
        await browser.close();
        console.log("Navegador fechado");
      }
    }
  }
  
  testBrowser();
  