# monitoring/generate_dashboards.py
import json
import os
from pathlib import Path


def create_connection_exhaustion_dashboard():
    """
    Generate dashboard specifically for connection exhaustion scenario
    """
    dashboard = {
        "title": "Lab 01 - Connection Pool Exhaustion",
        "uid": "lab01-connection-exhaustion",
        "tags": ["chaos-lab", "redis", "connections"],
        "timezone": "browser",
        "schemaVersion": 16,
        "version": 0,
        "refresh": "5s",
        "panels": [
            # Panel 1: Connection Usage Overview
            {
                "id": 1,
                "title": "Connection Pool Usage",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
                "targets": [
                    {
                        "expr": "redis_connected_clients",
                        "legendFormat": "Connected Clients",
                        "refId": "A",
                    },
                    {
                        "expr": "redis_config_maxclients",
                        "legendFormat": "Max Clients Limit",
                        "refId": "B",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "axisLabel": "Connections",
                            "fillOpacity": 10,
                            "lineWidth": 2,
                        },
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 2000},
                                {"color": "red", "value": 2400},
                            ],
                        },
                    }
                },
                "options": {
                    "tooltip": {"mode": "multi"},
                    "legend": {"displayMode": "list", "placement": "bottom"},
                },
            },
            # Panel 2: Usage Percentage Gauge
            {
                "id": 2,
                "title": "Connection Pool Usage %",
                "type": "gauge",
                "gridPos": {"h": 8, "w": 6, "x": 12, "y": 0},
                "targets": [
                    {
                        "expr": "redis_connected_clients / redis_config_maxclients * 100",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "percent",
                        "min": 0,
                        "max": 100,
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 50},
                                {"color": "orange", "value": 80},
                                {"color": "red", "value": 95},
                            ],
                        },
                    }
                },
                "options": {"showThresholdLabels": True, "showThresholdMarkers": True},
            },
            # Panel 3: Rejected Connections (Critical!)
            {
                "id": 3,
                "title": "Rejected Connections (FAILURE INDICATOR)",
                "type": "stat",
                "gridPos": {"h": 8, "w": 6, "x": 18, "y": 0},
                "targets": [
                    {
                        "expr": "redis_rejected_connections_total",
                        "legendFormat": "Total Rejected",
                        "refId": "A",
                    },
                    {
                        "expr": "increase(redis_rejected_connections_total[1m])",
                        "legendFormat": "Last Minute",
                        "refId": "B",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "red", "value": 1},
                            ],
                        },
                    }
                },
                "options": {
                    "colorMode": "background",
                    "graphMode": "none",
                    "textMode": "value_and_name",
                },
            },
            # Panel 4: Connection Rate
            {
                "id": 4,
                "title": "Connection Activity",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
                "targets": [
                    {
                        "expr": "rate(redis_connections_received_total[30s])",
                        "legendFormat": "New Connections/sec",
                        "refId": "A",
                    },
                    {
                        "expr": "rate(redis_total_commands_processed[30s])",
                        "legendFormat": "Commands/sec",
                        "refId": "B",
                    },
                ],
                "fieldConfig": {"defaults": {"custom": {"axisLabel": "Per Second"}}},
            },
            # Panel 5: Commands Processed (Throughput)
            {
                "id": 5,
                "title": "Redis Throughput (Commands/sec)",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
                "targets": [
                    {
                        "expr": "rate(redis_commands_processed_total[30s])",
                        "legendFormat": "Commands/sec",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {"fillOpacity": 20},
                    }
                },
            },
            # Panel 6: Correlation View
            {
                "id": 6,
                "title": "🔍 Correlation: Connection Usage vs Throughput",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 24, "x": 0, "y": 16},
                "targets": [
                    {
                        "expr": "redis_connected_clients / redis_config_maxclients * 100",
                        "legendFormat": "Connection Usage %",
                        "refId": "A",
                    },
                    {
                        "expr": "rate(redis_commands_processed_total[30s])",
                        "legendFormat": "Throughput (commands/sec)",
                        "refId": "B",
                    },
                ],
                "fieldConfig": {
                    "overrides": [
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Throughput (commands/sec)",
                            },
                            "properties": [
                                {"id": "custom.axisPlacement", "value": "right"}
                            ],
                        }
                    ]
                },
                "options": {"tooltip": {"mode": "multi"}},
            },
            # Panel 7: Current Stats Table
            {
                "id": 7,
                "title": "Current Connection Statistics",
                "type": "table",
                "gridPos": {"h": 6, "w": 24, "x": 0, "y": 24},
                "targets": [
                    {
                        "expr": "redis_connected_clients",
                        "legendFormat": "Connected Clients",
                        "refId": "A",
                        "instant": True,
                    },
                    {
                        "expr": "redis_config_maxclients",
                        "legendFormat": "Max Clients",
                        "refId": "B",
                        "instant": True,
                    },
                    {
                        "expr": "redis_blocked_clients",
                        "legendFormat": "Blocked Clients",
                        "refId": "C",
                        "instant": True,
                    },
                    {
                        "expr": "redis_rejected_connections_total",
                        "legendFormat": "Total Rejected",
                        "refId": "D",
                        "instant": True,
                    },
                ],
                "transformations": [{"id": "seriesToColumns", "options": {}}],
            },
        ],
    }

    return dashboard


def save_dashboard(dashboard, filename="lab01-connection-exhaustion.json"):
    """Save dashboard to file"""
    # Wrap in the format Grafana expects
    output = {"dashboard": dashboard, "overwrite": True, "inputs": [], "folderId": 0}

    # Ensure directory exists
    target_dir = Path("monitoring/dashboards")
    target_dir.mkdir(parents=True, exist_ok=True)

    filepath = target_dir / filename

    with filepath.open("w") as f:
        json.dump(output, f, indent=2)

    print(f"✓ Dashboard saved to: {filepath}")
    return filepath


def main():
    print("Generating Connection Exhaustion Dashboard...")
    print("-" * 60)

    dashboard = create_connection_exhaustion_dashboard()
    filepath = save_dashboard(dashboard)

    print("\nDashboard generated successfully!")
    print("\nNext steps:")
    print("  1. Make sure docker-compose is running: docker-compose up -d")
    print("  2. Dashboard will auto-load in Grafana")
    print("  3. Open: http://localhost:3000")
    print("  4. Navigate to: Dashboards → Lab 01 - Connection Pool Exhaustion")
    print("\nOr import manually:")
    print("  - Go to Grafana → Dashboards → Import")
    print(f"  - Upload: {filepath}")


if __name__ == "__main__":
    main()
