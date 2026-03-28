const http = require('http');
const https = require('https');
const fs = require('fs');

const BASE_URL = 'https://harvai.westeurope.cloudapp.azure.com';
const REPORT_ID = 'cbf97b0a-457d-4b4f-8913-547e0cdf390c';
const OUTPUT_FILE = '/workspace/claude-smoke/report_image.png';

// Read credentials from environment
const username = process.env.VIYA_USERNAME || process.env.SAS_USERNAME || process.env.VIYA_USER;
const password = process.env.VIYA_PASSWORD || process.env.SAS_PASSWORD || process.env.VIYA_PASS;

if (!username || !password) {
  console.error('ERROR: credentials not found in environment.');
  console.error('Set VIYA_USERNAME and VIYA_PASSWORD (or SAS_USERNAME/SAS_PASSWORD).');
  process.exit(1);
}
console.log(`Using username: ${username}`);

function request(method, url, headers = {}, body = null) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const lib = parsed.protocol === 'https:' ? https : http;
    const options = {
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method,
      headers,
      rejectUnauthorized: false,
    };
    const req = lib.request(options, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function main() {
  // Step 1: Authenticate
  console.log('\n--- Step 1: Authenticate ---');
  const tokenBody = new URLSearchParams({
    grant_type: 'password',
    username,
    password,
    client_id: 'sas.ec',
    client_secret: '',
  }).toString();

  const tokenRes = await request(
    'POST',
    `${BASE_URL}/SASLogon/oauth/token`,
    {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(tokenBody),
    },
    tokenBody
  );

  if (tokenRes.status !== 200) {
    console.error(`Auth failed (${tokenRes.status}):`, tokenRes.body.toString());
    process.exit(1);
  }

  const tokenData = JSON.parse(tokenRes.body.toString());
  const accessToken = tokenData.access_token;
  console.log('Authenticated successfully. Token type:', tokenData.token_type);

  // Save token for reference
  fs.writeFileSync('/workspace/claude-smoke/token.json', JSON.stringify(tokenData, null, 2));
  console.log('Token saved to token.json');

  // Step 2: Request report image job
  console.log('\n--- Step 2: Create reportImages job ---');
  const jobPayload = JSON.stringify({
    reportUri: `/reports/reports/${REPORT_ID}`,
    layoutType: 'entireSection',
    selectionType: 'report',
    size: '1920x1080',
    imageType: 'png',
  });

  const jobRes = await request(
    'POST',
    `${BASE_URL}/reportImages/jobs`,
    {
      'Content-Type': 'application/vnd.sas.report.images.job.request+json',
      'Accept': 'application/vnd.sas.report.images.job+json',
      'Authorization': `Bearer ${accessToken}`,
      'Content-Length': Buffer.byteLength(jobPayload),
    },
    jobPayload
  );

  if (jobRes.status !== 200 && jobRes.status !== 201 && jobRes.status !== 202) {
    console.error(`Job creation failed (${jobRes.status}):`, jobRes.body.toString());
    process.exit(1);
  }

  const jobData = JSON.parse(jobRes.body.toString());
  const jobId = jobData.id;
  console.log(`Job created: ${jobId}, state: ${jobData.state}`);

  // Step 3: Poll until complete
  console.log('\n--- Step 3: Polling for completion ---');
  let state = jobData.state;
  let pollData = jobData;
  let attempts = 0;

  while (state !== 'completed' && state !== 'failed' && attempts < 30) {
    await sleep(2000);
    attempts++;
    const pollRes = await request(
      'GET',
      `${BASE_URL}/reportImages/jobs/${jobId}`,
      {
        'Accept': 'application/vnd.sas.report.images.job+json',
        'Authorization': `Bearer ${accessToken}`,
      }
    );
    pollData = JSON.parse(pollRes.body.toString());
    state = pollData.state;
    console.log(`  Attempt ${attempts}: state = ${state}`);
  }

  if (state !== 'completed') {
    console.error('Job did not complete. Final state:', state);
    console.error('Response:', JSON.stringify(pollData, null, 2));
    process.exit(1);
  }

  // Step 4: Extract image URL and download
  console.log('\n--- Step 4: Download image ---');
  console.log('Job response:', JSON.stringify(pollData, null, 2));

  // The image URL is typically in images[0].imageUrl or links
  const images = pollData.images || [];
  if (images.length === 0) {
    console.error('No images in job response');
    process.exit(1);
  }

  const imgLink = images[0].links && images[0].links.find(l => l.rel === 'image');
  const imageUrl = images[0].imageUrl || images[0].url || (imgLink && imgLink.href);
  console.log('Image URL:', imageUrl);

  const fullUrl = imageUrl.startsWith('http') ? imageUrl : `${BASE_URL}${imageUrl}`;
  const imgRes = await request('GET', fullUrl, {
    'Authorization': `Bearer ${accessToken}`,
  });

  if (imgRes.status !== 200) {
    console.error(`Image download failed (${imgRes.status})`);
    process.exit(1);
  }

  fs.writeFileSync(OUTPUT_FILE, imgRes.body);
  console.log(`\nSuccess! PNG saved to: ${OUTPUT_FILE} (${imgRes.body.length} bytes)`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
