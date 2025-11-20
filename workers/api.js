/**
 * Cloudflare Worker - Serve screener.json from R2
 * Deploy: wrangler deploy
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    
    // CORS headers
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };
    
    // Handle OPTIONS for CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }
    
    // Route: GET /api/screener
    if (url.pathname === '/api/screener') {
      try {
        // Fetch screener.json from R2 bucket
        const object = await env.R2_BUCKET.get('screener.json');
        
        if (object === null) {
          return new Response(
            JSON.stringify({ error: 'Screener data not found' }), 
            { 
              status: 404,
              headers: {
                ...corsHeaders,
                'Content-Type': 'application/json'
              }
            }
          );
        }
        
        // Return JSON with cache headers
        return new Response(await object.text(), {
          headers: {
            ...corsHeaders,
            'Content-Type': 'application/json',
            'Cache-Control': 'public, max-age=3600', // Cache 1 hour
          }
        });
        
      } catch (error) {
        return new Response(
          JSON.stringify({ error: 'Internal server error', details: error.message }), 
          { 
            status: 500,
            headers: {
              ...corsHeaders,
              'Content-Type': 'application/json'
            }
          }
        );
      }
    }
    
    // Route: GET /health
    if (url.pathname === '/health') {
      return new Response(
        JSON.stringify({ status: 'ok', timestamp: new Date().toISOString() }),
        {
          headers: {
            ...corsHeaders,
            'Content-Type': 'application/json'
          }
        }
      );
    }
    
    // 404 for other routes
    return new Response(
      JSON.stringify({ error: 'Not found' }), 
      { 
        status: 404,
        headers: {
          ...corsHeaders,
          'Content-Type': 'application/json'
        }
      }
    );
  }
}
