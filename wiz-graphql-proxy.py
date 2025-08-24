#!/usr/bin/env python3
"""
Wiz GraphQL Proxy Service
Handles OAuth token refresh and proxies GraphQL requests to Wiz API
"""

import os
import time
import json
import logging
from flask import Flask, request, jsonify, Response
import requests
from datetime import datetime, timedelta

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
WIZ_AUTH_URL = os.environ.get('WIZ_AUTH_URL', 'https://auth.app.wiz.io/oauth/token')
WIZ_GRAPHQL_URL = os.environ.get('WIZ_GRAPHQL_URL', 'https://api.us48.app.wiz.io/graphql')
WIZ_CLIENT_ID = os.environ.get('WIZ_CLIENT_ID')
WIZ_CLIENT_SECRET = os.environ.get('WIZ_CLIENT_SECRET')
WIZ_AUDIENCE = os.environ.get('WIZ_AUDIENCE', 'wiz-api')

# Token cache
token_cache = {
    'access_token': None,
    'expires_at': None
}

def get_wiz_token():
    """Get a valid Wiz access token, refreshing if necessary"""
    global token_cache
    
    # Check if we have a valid token
    if token_cache['access_token'] and token_cache['expires_at']:
        if datetime.now() < token_cache['expires_at']:
            logger.info("Using cached token")
            return token_cache['access_token']
    
    logger.info("Fetching new token from Wiz")
    
    # Request new token
    try:
        response = requests.post(
            WIZ_AUTH_URL,
            headers={
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            data={
                'grant_type': 'client_credentials',
                'client_id': WIZ_CLIENT_ID,
                'client_secret': WIZ_CLIENT_SECRET,
                'audience': WIZ_AUDIENCE
            },
            timeout=30
        )
        response.raise_for_status()
        
        token_data = response.json()
        token_cache['access_token'] = token_data['access_token']
        
        # Set expiration time (subtract 60 seconds for safety)
        expires_in = token_data.get('expires_in', 3600) - 60
        token_cache['expires_at'] = datetime.now() + timedelta(seconds=expires_in)
        
        logger.info(f"Token refreshed, expires at {token_cache['expires_at']}")
        return token_cache['access_token']
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get Wiz token: {e}")
        raise

@app.route('/graphql', methods=['POST', 'OPTIONS'])
def proxy_graphql():
    """Proxy GraphQL requests to Wiz API with authentication"""
    
    # Handle CORS preflight
    if request.method == 'OPTIONS':
        return Response(status=200, headers={
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization'
        })
    
    try:
        # Get valid token
        token = get_wiz_token()
        
        # Forward the GraphQL request to Wiz
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        # Get the GraphQL query from the request
        graphql_query = request.get_json()
        
        logger.info(f"Proxying GraphQL query to Wiz")
        
        # Make request to Wiz
        response = requests.post(
            WIZ_GRAPHQL_URL,
            headers=headers,
            json=graphql_query,
            timeout=60
        )
        
        # Return Wiz response
        return Response(
            response.content,
            status=response.status_code,
            headers={
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        )
        
    except Exception as e:
        logger.error(f"Proxy error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200

@app.route('/introspection', methods=['POST'])
def introspection():
    """Handle GraphQL introspection queries"""
    return proxy_graphql()

if __name__ == '__main__':
    if not WIZ_CLIENT_ID or not WIZ_CLIENT_SECRET:
        logger.error("WIZ_CLIENT_ID and WIZ_CLIENT_SECRET must be set")
        exit(1)
    
    app.run(host='0.0.0.0', port=8080, debug=False)