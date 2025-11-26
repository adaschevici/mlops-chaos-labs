# monitoring/generate_dashboards.py
import json
from pathlib import Path


def create_connection_exhaustion_dashboard():
    """
    Generate dashboard compatible with latest Grafana
    """
    dashboard = {
        "title": "Lab 01 - Connection Pool Exhaustion",
        "uid": "lab01-connection-exhaustion",
        "tags": ["chaos-lab", "redis", "connections"],
        "timezone": "browser",
        "schemaVersion": 38,  # Updated for latest Grafana
        "version": 0,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "timepicker": {},
        "fiscalYearStartMonth": 0,
        "panels": [
            # Panel 1: Connection Pool Usage
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 1,
                "title": "Connection Pool Usage",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "redis_connected_clients",
                        "legendFormat": "Connected Clients",
                        "refId": "A",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "redis_config_maxclients",
                        "legendFormat": "Max Clients Limit",
                        "refId": "B",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "axisCenteredZero": False,
                            "axisColorMode": "text",
                            "axisLabel": "Connections",
                            "axisPlacement": "auto",
                            "fillOpacity": 10,
                            "gradientMode": "none",
                            "lineWidth": 2,
                            "spanNulls": False,
                            "drawStyle": "line",
                            "lineInterpolation": "linear",
                            "barAlignment": 0,
                            "showPoints": "never",
                            "pointSize": 5,
                            "stacking": {"mode": "none", "group": "A"},
                            "hideFrom": {
                                "tooltip": False,
                                "viz": False,
                                "legend": False,
                            },
                        },
                        "mappings": [],
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 2000},
                                {"color": "red", "value": 2400},
                            ],
                        },
                    },
                    "overrides": [],
                },
                "options": {
                    "tooltip": {"mode": "multi", "sort": "none"},
                    "legend": {
                        "showLegend": True,
                        "displayMode": "list",
                        "placement": "bottom",
                        "calcs": [],
                    },
                },
            },
            # Panel 2: Usage Percentage Gauge
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 2,
                "title": "Connection Pool Usage %",
                "type": "gauge",
                "gridPos": {"h": 8, "w": 6, "x": 12, "y": 0},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
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
                        "mappings": [],
                    },
                    "overrides": [],
                },
                "options": {
                    "orientation": "auto",
                    "showThresholdLabels": True,
                    "showThresholdMarkers": True,
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"]},
                },
            },
            # Panel 3: Rejected Connections
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 3,
                "title": "Rejected Connections (FAILURE INDICATOR)",
                "type": "stat",
                "gridPos": {"h": 8, "w": 6, "x": 18, "y": 0},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "redis_rejected_connections_total",
                        "legendFormat": "Total Rejected",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "mappings": [],
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "red", "value": 1},
                            ],
                        },
                    },
                    "overrides": [],
                },
                "options": {
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"]},
                    "orientation": "auto",
                    "textMode": "auto",
                    "colorMode": "background",
                    "graphMode": "area",
                    "justifyMode": "auto",
                },
            },
            # Panel 4: Connection Activity
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 4,
                "title": "Connection Activity",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "rate(redis_connections_received_total[30s])",
                        "legendFormat": "New Connections/sec",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "axisCenteredZero": False,
                            "axisColorMode": "text",
                            "axisLabel": "Per Second",
                            "axisPlacement": "auto",
                            "drawStyle": "line",
                            "fillOpacity": 10,
                            "gradientMode": "none",
                            "lineInterpolation": "linear",
                            "lineWidth": 1,
                            "pointSize": 5,
                            "showPoints": "never",
                            "spanNulls": False,
                        },
                        "mappings": [],
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [{"color": "green", "value": None}],
                        },
                    },
                    "overrides": [],
                },
                "options": {
                    "legend": {
                        "calcs": [],
                        "displayMode": "list",
                        "placement": "bottom",
                        "showLegend": True,
                    },
                    "tooltip": {"mode": "multi", "sort": "none"},
                },
            },
            # Panel 5: Redis Throughput
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 5,
                "title": "Redis Throughput (Commands/sec)",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "rate(redis_commands_processed_total[30s])",
                        "legendFormat": "Commands/sec",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "axisCenteredZero": False,
                            "axisColorMode": "text",
                            "axisLabel": "",
                            "axisPlacement": "auto",
                            "drawStyle": "line",
                            "fillOpacity": 20,
                            "gradientMode": "none",
                            "lineInterpolation": "linear",
                            "lineWidth": 1,
                            "pointSize": 5,
                            "showPoints": "never",
                            "spanNulls": False,
                        },
                        "mappings": [],
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [{"color": "green", "value": None}],
                        },
                    },
                    "overrides": [],
                },
                "options": {
                    "legend": {
                        "calcs": [],
                        "displayMode": "list",
                        "placement": "bottom",
                        "showLegend": True,
                    },
                    "tooltip": {"mode": "multi", "sort": "none"},
                },
            },
            # Panel 6: Correlation View
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 6,
                "title": "🔍 Correlation: Connection Usage vs Throughput",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 24, "x": 0, "y": 16},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "redis_connected_clients / redis_config_maxclients * 100",
                        "legendFormat": "Connection Usage %",
                        "refId": "A",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "rate(redis_commands_processed_total[30s])",
                        "legendFormat": "Throughput (commands/sec)",
                        "refId": "B",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "axisCenteredZero": False,
                            "axisColorMode": "text",
                            "axisLabel": "",
                            "axisPlacement": "auto",
                            "drawStyle": "line",
                            "fillOpacity": 10,
                            "gradientMode": "none",
                            "lineInterpolation": "linear",
                            "lineWidth": 1,
                            "pointSize": 5,
                            "showPoints": "never",
                            "spanNulls": False,
                        },
                        "mappings": [],
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [{"color": "green", "value": None}],
                        },
                    },
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
                    ],
                },
                "options": {
                    "legend": {
                        "calcs": [],
                        "displayMode": "list",
                        "placement": "bottom",
                        "showLegend": True,
                    },
                    "tooltip": {"mode": "multi", "sort": "none"},
                },
            },
            # Panel 7: Current Stats Table
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 7,
                "title": "Current Connection Statistics",
                "type": "table",
                "gridPos": {"h": 6, "w": 24, "x": 0, "y": 24},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "redis_connected_clients",
                        "legendFormat": "Connected Clients",
                        "refId": "A",
                        "instant": True,
                        "format": "table",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "redis_config_maxclients",
                        "legendFormat": "Max Clients",
                        "refId": "B",
                        "instant": True,
                        "format": "table",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "redis_blocked_clients",
                        "legendFormat": "Blocked Clients",
                        "refId": "C",
                        "instant": True,
                        "format": "table",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "redis_rejected_connections_total",
                        "legendFormat": "Total Rejected",
                        "refId": "D",
                        "instant": True,
                        "format": "table",
                    },
                ],
                "transformations": [{"id": "merge", "options": {}}],
                "options": {
                    "showHeader": True,
                    "cellHeight": "sm",
                    "footer": {
                        "show": False,
                        "reducer": ["sum"],
                        "countRows": False,
                        "fields": "",
                    },
                },
                "fieldConfig": {
                    "defaults": {
                        "custom": {
                            "align": "auto",
                            "cellOptions": {"type": "auto"},
                            "inspect": False,
                        },
                        "mappings": [],
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [{"color": "green", "value": None}],
                        },
                    },
                    "overrides": [],
                },
            },
        ],
    }

    return dashboard


def save_dashboard(dashboard, filename="lab01-connection-exhaustion.json"):
    """Save dashboard to file in Grafana import format"""
    # Ensure directory exists
    target_dir = Path("monitoring/dashboards")
    target_dir.mkdir(parents=True, exist_ok=True)

    filepath = target_dir / filename

    # Write the dashboard directly (no wrapper needed for file provisioning)
    with filepath.open("w") as f:
        json.dump(dashboard, f, indent=2)

    print(f"✓ Dashboard saved to: {filepath}")
    return filepath


def main():
    print("Generating Connection Exhaustion Dashboard...")
    print("-" * 60)

    dashboard = create_connection_exhaustion_dashboard()
    _filepath = save_dashboard(dashboard)

    print("\nDashboard generated successfully!")
    print("\nNext steps:")
    print("  1. Restart Grafana: docker-compose restart grafana")
    print("  2. Wait 10 seconds for provisioning")
    print("  3. Open: http://localhost:3000")
    print("  4. Navigate to: Dashboards → Lab 01 - Connection Pool Exhaustion")


if __name__ == "__main__":
    main()
