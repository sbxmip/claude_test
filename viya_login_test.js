const { chromium } = require('playwright');

(async () => {
  const username = process.env.VIYA_USERNAME;
  const password = process.env.VIYA_PASSWORD;

  if (!username || !password) {
    console.error('ERROR: VIYA_USERNAME and VIYA_PASSWORD environment variables must be set');
    process.exit(1);
  }

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ ignoreHTTPSErrors: true });
  const page = await context.newPage();

  console.log('Navigating to SAS Viya login page...');
  await page.goto('https://harvai.westeurope.cloudapp.azure.com/SASLogon/login', {
    waitUntil: 'domcontentloaded',
    timeout: 60000
  });

  console.log('Title before login:', await page.title());
  console.log('URL before login:', page.url());

  await page.screenshot({ path: '/workspace/claude-smoke/viya-before-login.png', fullPage: true });
  console.log('Screenshot saved: viya-before-login.png');

  // Fill in credentials
  await page.fill('input[name="username"], input[id="username"], input[type="text"]', username);
  await page.fill('input[name="password"], input[id="password"], input[type="password"]', password);

  console.log('Credentials entered, submitting...');
  await Promise.all([
    page.waitForNavigation({ timeout: 30000 }).catch(() => {}),
    page.click('#submitBtn')
  ]);

  // Wait a moment for the page to settle
  await page.waitForTimeout(3000);

  console.log('Title after login:', await page.title());
  console.log('URL after login:', page.url());

  await page.screenshot({ path: '/workspace/claude-smoke/viya-after-login.png', fullPage: true });
  console.log('Screenshot saved: viya-after-login.png');

  await browser.close();
  console.log('Done.');
})();
