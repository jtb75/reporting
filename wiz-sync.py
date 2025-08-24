#!/usr/bin/env python3
"""
Sync Wiz issues directly to PostgreSQL for Grafana visualization
"""

import os
import json
import psycopg2
import requests
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
WIZ_PROXY_URL = os.environ.get('WIZ_PROXY_URL', 'http://wiz-graphql-proxy.reporting.svc.cluster.local/graphql')
PG_HOST = os.environ.get('PG_HOST', 'postgres.reporting.svc.cluster.local')
PG_DB = os.environ.get('PG_DB', 'reporting')
PG_USER = os.environ.get('PG_USER', 'postgres')
PG_PASSWORD = os.environ.get('PG_PASSWORD', 'ReportingDB2024!Secure')

def fetch_wiz_issues():
    """Fetch Wiz issues directly via proxy"""
    query = """
    query GetWizIssues {
      issuesV2(first: 100) {
        nodes {
          id
          severity
          status
          createdAt
          entitySnapshot {
            name
            type
          }
        }
        totalCount
      }
    }
    """
    
    headers = {
        'Content-Type': 'application/json'
    }
    
    response = requests.post(
        WIZ_PROXY_URL,
        json={'query': query},
        headers=headers
    )
    response.raise_for_status()
    return response.json()['data']['issuesV2']

def sync_to_postgres(issues_data):
    """Sync Wiz issues to PostgreSQL"""
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    
    try:
        with conn.cursor() as cur:
            # Create table if not exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wiz_issues (
                    id VARCHAR(255) PRIMARY KEY,
                    severity VARCHAR(50),
                    status VARCHAR(50),
                    created_at TIMESTAMPTZ,
                    entity_name VARCHAR(255),
                    entity_type VARCHAR(100),
                    last_synced TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            
            # Create index
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_wiz_issues_severity 
                ON wiz_issues(severity)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_wiz_issues_status 
                ON wiz_issues(status)
            """)
            
            # Clear old data
            cur.execute("TRUNCATE TABLE wiz_issues")
            
            # Insert new data
            for issue in issues_data['nodes']:
                cur.execute("""
                    INSERT INTO wiz_issues 
                    (id, severity, status, created_at, entity_name, entity_type)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        severity = EXCLUDED.severity,
                        status = EXCLUDED.status,
                        entity_name = EXCLUDED.entity_name,
                        entity_type = EXCLUDED.entity_type,
                        last_synced = NOW()
                """, (
                    issue['id'],
                    issue['severity'],
                    issue['status'],
                    issue['createdAt'],
                    issue['entitySnapshot']['name'] if issue['entitySnapshot'] else None,
                    issue['entitySnapshot']['type'] if issue['entitySnapshot'] else None
                ))
            
            # Update metrics table
            cur.execute("""
                INSERT INTO metrics (metric_name, metric_value, labels)
                VALUES 
                    ('wiz_total_issues', %s, '{}'),
                    ('wiz_open_issues', 
                     (SELECT COUNT(*) FROM wiz_issues WHERE status = 'OPEN'), 
                     '{"status": "open"}'),
                    ('wiz_critical_issues',
                     (SELECT COUNT(*) FROM wiz_issues WHERE severity = 'CRITICAL'),
                     '{"severity": "critical"}')
                ON CONFLICT (metric_name, labels) DO UPDATE SET
                    metric_value = EXCLUDED.metric_value,
                    timestamp = NOW()
            """, (issues_data['totalCount'],))
            
            conn.commit()
            logger.info(f"Synced {len(issues_data['nodes'])} issues to PostgreSQL")
            
    finally:
        conn.close()

def main():
    try:
        logger.info("Starting Wiz data sync...")
        issues = fetch_wiz_issues()
        sync_to_postgres(issues)
        logger.info("Sync completed successfully")
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise

if __name__ == '__main__':
    main()