#!/usr/bin/env python3
"""
Sample data ingestion script for the reporting platform.
This demonstrates how to insert data via Hasura GraphQL API.
"""

import requests
import json
import random
from datetime import datetime, timedelta
import time

HASURA_URL = "https://hasura.k8s.ng20.org/v1/graphql"
ADMIN_SECRET = "HasuraAdmin2024!Secret"

headers = {
    "Content-Type": "application/json",
    "x-hasura-admin-secret": ADMIN_SECRET
}

def insert_security_event(event_type, severity, source_ip=None, namespace="default", details={}):
    """Insert a security event"""
    mutation = """
    mutation InsertSecurityEvent($event: security_events_insert_input!) {
      insert_security_events_one(object: $event) {
        id
        timestamp
      }
    }
    """
    
    variables = {
        "event": {
            "event_type": event_type,
            "severity": severity,
            "source_ip": source_ip,
            "namespace": namespace,
            "target_resource": f"pod/{namespace}/vulnerable-app",
            "details": details,
            "detection_source": "wiz-sensor"
        }
    }
    
    response = requests.post(HASURA_URL, 
                            json={"query": mutation, "variables": variables},
                            headers=headers)
    return response.json()

def insert_application_metric(app_name, metric_name, value, namespace="default"):
    """Insert an application metric"""
    mutation = """
    mutation InsertAppMetric($metric: application_metrics_insert_input!) {
      insert_application_metrics_one(object: $metric) {
        id
        timestamp
      }
    }
    """
    
    variables = {
        "metric": {
            "app_name": app_name,
            "namespace": namespace,
            "metric_name": metric_name,
            "metric_value": value,
            "unit": "count",
            "labels": {"environment": "production", "version": "v1"}
        }
    }
    
    response = requests.post(HASURA_URL,
                            json={"query": mutation, "variables": variables},
                            headers=headers)
    return response.json()

def insert_vulnerability(resource_name, vulnerability_id, severity, cvss_score):
    """Insert a vulnerability finding"""
    mutation = """
    mutation InsertVulnerability($vuln: vulnerability_findings_insert_input!) {
      insert_vulnerability_findings_one(object: $vuln) {
        id
        discovered_at
      }
    }
    """
    
    variables = {
        "vuln": {
            "resource_type": "container",
            "resource_name": resource_name,
            "namespace": "targetapp",
            "vulnerability_id": vulnerability_id,
            "severity": severity,
            "cvss_score": cvss_score,
            "description": f"Vulnerability {vulnerability_id} detected in {resource_name}",
            "remediation": "Update to latest version",
            "status": "open"
        }
    }
    
    response = requests.post(HASURA_URL,
                            json={"query": mutation, "variables": variables},
                            headers=headers)
    return response.json()

def insert_api_request(method, path, status_code, response_time_ms):
    """Insert an API request log"""
    mutation = """
    mutation InsertAPIRequest($request: api_requests_insert_input!) {
      insert_api_requests_one(object: $request) {
        id
        timestamp
      }
    }
    """
    
    variables = {
        "request": {
            "method": method,
            "path": path,
            "status_code": status_code,
            "response_time_ms": response_time_ms,
            "client_ip": f"192.168.1.{random.randint(1, 254)}",
            "user_id": f"user_{random.randint(1, 100)}",
            "request_headers": {"user-agent": "Mozilla/5.0", "accept": "application/json"}
        }
    }
    
    response = requests.post(HASURA_URL,
                            json={"query": mutation, "variables": variables},
                            headers=headers)
    return response.json()

def insert_performance_metric(node_name, pod_name, namespace, cpu_cores, memory_bytes):
    """Insert a performance metric"""
    mutation = """
    mutation InsertPerfMetric($metric: performance_metrics_insert_input!) {
      insert_performance_metrics_one(object: $metric) {
        id
        timestamp
      }
    }
    """
    
    variables = {
        "metric": {
            "node_name": node_name,
            "pod_name": pod_name,
            "namespace": namespace,
            "container_name": "app",
            "cpu_usage_cores": cpu_cores,
            "memory_usage_bytes": memory_bytes,
            "network_rx_bytes": random.randint(1000000, 10000000),
            "network_tx_bytes": random.randint(1000000, 10000000),
            "labels": {"app": pod_name.split("-")[0]}
        }
    }
    
    response = requests.post(HASURA_URL,
                            json={"query": mutation, "variables": variables},
                            headers=headers)
    return response.json()

def generate_sample_data():
    """Generate various types of sample data"""
    
    print("🚀 Starting data ingestion...")
    
    # Security Events
    security_events = [
        ("unauthorized_access", "high", "192.168.1.100"),
        ("command_injection", "critical", "192.168.1.101"),
        ("sql_injection", "high", "192.168.1.102"),
        ("privilege_escalation", "critical", "10.0.0.50"),
        ("suspicious_network", "medium", "172.16.0.10"),
        ("file_integrity", "low", None),
    ]
    
    print("\n📊 Inserting security events...")
    for event_type, severity, source_ip in security_events:
        result = insert_security_event(
            event_type=event_type,
            severity=severity,
            source_ip=source_ip,
            namespace="targetapp" if "injection" in event_type else "default",
            details={"detected": True, "blocked": severity in ["critical", "high"]}
        )
        if "data" in result:
            print(f"  ✅ Inserted {event_type} event (ID: {result['data']['insert_security_events_one']['id']})")
        else:
            print(f"  ❌ Failed to insert {event_type}: {result}")
        time.sleep(0.5)
    
    # Application Metrics
    print("\n📈 Inserting application metrics...")
    apps = ["targetapp", "harbor", "hasura", "grafana"]
    metrics = ["request_count", "error_rate", "response_time", "active_connections"]
    
    for app in apps:
        for metric in metrics:
            value = random.uniform(0, 100) if "rate" in metric else random.randint(1, 1000)
            result = insert_application_metric(app, metric, value, namespace="default")
            if "data" in result:
                print(f"  ✅ Inserted {metric} for {app}")
            time.sleep(0.2)
    
    # Vulnerability Findings
    print("\n🔍 Inserting vulnerability findings...")
    vulnerabilities = [
        ("targetapp:latest", "CVE-2024-1234", "critical", 9.8),
        ("targetapp:latest", "CVE-2024-5678", "high", 7.5),
        ("postgres:15", "CVE-2024-9012", "medium", 5.3),
        ("hasura:v2.36.0", "CVE-2024-3456", "low", 3.1),
    ]
    
    for resource, vuln_id, severity, cvss in vulnerabilities:
        result = insert_vulnerability(resource, vuln_id, severity, cvss)
        if "data" in result:
            print(f"  ✅ Inserted {vuln_id} for {resource}")
        time.sleep(0.3)
    
    # API Requests
    print("\n🌐 Inserting API request logs...")
    endpoints = ["/api/health", "/api/metrics", "/v1/graphql", "/api/deserialize", "/ping"]
    methods = ["GET", "POST", "GET", "POST", "GET"]
    
    for i in range(20):
        idx = random.randint(0, len(endpoints)-1)
        status = random.choice([200, 200, 200, 201, 400, 401, 404, 500])
        response_time = random.randint(10, 2000) if status == 200 else random.randint(100, 5000)
        
        result = insert_api_request(methods[idx], endpoints[idx], status, response_time)
        if "data" in result:
            print(f"  ✅ {methods[idx]} {endpoints[idx]} -> {status} ({response_time}ms)")
        time.sleep(0.1)
    
    # Performance Metrics
    print("\n⚡ Inserting performance metrics...")
    nodes = ["kube01", "kube02", "kube03", "kube04"]
    pods = [
        ("targetapp-abc123", "targetapp"),
        ("hasura-def456", "reporting"),
        ("postgres-0", "reporting"),
        ("grafana-ghi789", "observability-stack")
    ]
    
    for node in nodes:
        for pod_name, namespace in pods:
            cpu = random.uniform(0.1, 2.0)
            memory = random.randint(100000000, 2000000000)  # 100MB to 2GB
            
            result = insert_performance_metric(node, pod_name, namespace, cpu, memory)
            if "data" in result:
                print(f"  ✅ Metrics for {pod_name} on {node}")
            time.sleep(0.2)
    
    print("\n✨ Data ingestion complete!")
    
    # Query to verify
    print("\n📊 Verifying data...")
    query = """
    query VerifyData {
      security_events_aggregate {
        aggregate {
          count
        }
      }
      application_metrics_aggregate {
        aggregate {
          count
        }
      }
      vulnerability_findings_aggregate {
        aggregate {
          count
        }
      }
      api_requests_aggregate {
        aggregate {
          count
        }
      }
      performance_metrics_aggregate {
        aggregate {
          count
        }
      }
    }
    """
    
    response = requests.post(HASURA_URL,
                            json={"query": query},
                            headers=headers)
    result = response.json()
    
    if "data" in result:
        data = result["data"]
        print(f"  Security Events: {data['security_events_aggregate']['aggregate']['count']}")
        print(f"  Application Metrics: {data['application_metrics_aggregate']['aggregate']['count']}")
        print(f"  Vulnerabilities: {data['vulnerability_findings_aggregate']['aggregate']['count']}")
        print(f"  API Requests: {data['api_requests_aggregate']['aggregate']['count']}")
        print(f"  Performance Metrics: {data['performance_metrics_aggregate']['aggregate']['count']}")

if __name__ == "__main__":
    generate_sample_data()