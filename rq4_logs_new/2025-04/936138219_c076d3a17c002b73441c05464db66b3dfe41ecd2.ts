import http from 'http';
import https from 'https';
import dotenv from 'dotenv';

dotenv.config({ path: './.env' });

const server = http.createServer((req, res) => {
  const reqUrl = req.url || '';
  const [path, searchParams] = reqUrl.split('&');
  const [routeSegment, ...segments] = path.replace(/^\//, '').split('/');
  const route = process.env[`${routeSegment.toUpperCase()}_API_URL`];

  if (!route) {
    res.writeHead(502);
    res.end('Cannot process request');
    return;
  }

  const targetUrl = new URL(
    route + (segments.length ? `/${segments.join('/')}` : '') + (searchParams ? `?${searchParams}` : ''),
  );

  const isHttps = targetUrl.protocol === 'https:';

  const options = {
    hostname: targetUrl.hostname,
    servername: targetUrl.hostname,
    path: targetUrl.pathname + targetUrl.search,
    port: isHttps ? 443 : 80,
    method: req.method,
    headers: {
      ...req.headers,
      host: targetUrl.hostname,
      ...(isHttps
        ? {
            // Add recommended headers for AWS
            'X-Forwarded-Proto': 'https',
            'X-Forwarded-Port': '443',
            'X-Forwarded-For': req.socket.remoteAddress,
          }
        : {}),
    },
    rejectUnauthorized: false,
    changeOrigin: true,
  };

  const request = isHttps ? https.request : http.request;

  // Create the proxy request
  const proxyReq = request(options, proxyRes => {
    // Set response headers
    res.writeHead(proxyRes.statusCode || 200, proxyRes.headers);
    // Pipe the response from target to client
    proxyRes.pipe(res);
  });

  // Handle errors
  proxyReq.on('error', err => {
    console.error('Proxy Request Error:', err);
    res.writeHead(500);
    res.end('Proxy Error');
  });

  // Handle request body for POST, PUT, PATCH methods
  if (['POST', 'PUT', 'PATCH'].includes(req.method || '')) {
    let body = '';

    req.on('data', chunk => {
      body += chunk.toString();
    });

    req.on('end', () => {
      // Add Content-Length header if not present
      if (!req.headers['content-length']) {
        proxyReq.setHeader('Content-Length', Buffer.byteLength(body));
      }

      proxyReq.write(body);
      proxyReq.end();
    });

    // Handle request errors
    req.on('error', err => {
      console.error('Request Error:', err);
      proxyReq.end();
    });
  } else {
    // For GET, DELETE, etc., just end the request
    proxyReq.end();
  }
});

// Start the proxy server
const BFF_PORT = process.env.BFF_PORT || 80;
server.listen(BFF_PORT, () => {
  console.log(`BFF server running on port ${BFF_PORT}`);
});

// Global error handler
server.on('error', err => {
  console.error('Server error:', err);
});