# monitoring/generate_dashboards.py
import json
from pathlib import Path


def create_connection_exhaustion_dashboard():
    """
    Generate dashboard with Redis AND task throughput metrics
    """
    dashboard = {
        "title": "Lab 01 - Connection Pool Exhaustion Enhanced",
        "uid": "lab01-connection-exhaustion-enhanced",
        "tags": ["chaos-lab", "redis", "connections"],
        "timezone": "browser",
        "schemaVersion": 38,
        "version": 0,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "timepicker": {},
        "fiscalYearStartMonth": 0,
        "panels": [
            # Panel 1: Connection Pool Usage (existing)
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
                            "axisLabel": "Connections",
                            "fillOpacity": 10,
                            "lineWidth": 2,
                            "drawStyle": "line",
                            "lineInterpolation": "linear",
                        },
                    }
                },
            },
            # Panel 2: Usage % Gauge (existing)
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
                    }
                },
                "options": {"showThresholdLabels": True, "showThresholdMarkers": True},
            },
            # Panel 3: Rejected Connections (existing)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 3,
                "title": "Rejected Connections",
                "type": "stat",
                "gridPos": {"h": 8, "w": 6, "x": 18, "y": 0},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "redis_rejected_connections_total",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "red", "value": 1},
                            ]
                        }
                    }
                },
                "options": {"colorMode": "background", "graphMode": "area"},
            },
            # NEW Panel 4: Task Throughput (Tasks/sec)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 4,
                "title": "⚡ Task Throughput (Tasks/sec)",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(rate(celery_task_total{status='success'}[30s]))",
                        "legendFormat": "Successful Tasks/sec",
                        "refId": "A",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(rate(celery_task_total{status='failure'}[30s]))",
                        "legendFormat": "Failed Tasks/sec",
                        "refId": "B",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(rate(celery_task_total[30s]))",
                        "legendFormat": "Total Tasks/sec",
                        "refId": "C",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "axisLabel": "Tasks/Second",
                            "fillOpacity": 15,
                            "lineWidth": 2,
                            "drawStyle": "line",
                        },
                    },
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "Failed Tasks/sec"},
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "red", "mode": "fixed"},
                                }
                            ],
                        },
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Successful Tasks/sec",
                            },
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "green", "mode": "fixed"},
                                }
                            ],
                        },
                    ],
                },
            },
            # ------------------------------------------------------------------
            # UPDATED Panel 5: Current Task Rate Gauge (REVERSED LOGIC)
            # ------------------------------------------------------------------
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 5,
                "title": "Current Task Rate",
                "type": "gauge",
                "gridPos": {"h": 8, "w": 6, "x": 12, "y": 8},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(rate(celery_task_total[30s]))",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "ops",
                        "min": 0,
                        "max": 150,  # Increased to accommodate 118+ ops/s
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "red", "value": 0},  # 0-10: Critical
                                {"color": "orange", "value": 10},  # 10-30: Poor
                                {"color": "yellow", "value": 30},  # 30-70: Moderate
                                {"color": "green", "value": 70},  # 70+: Excellent
                            ],
                        },
                    }
                },
                "options": {
                    "showThresholdLabels": True,
                    "showThresholdMarkers": True,
                    "orientation": "auto",
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"]},
                },
            },
            # NEW Panel 6: Tasks In Progress
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 6,
                "title": "Tasks In Progress",
                "type": "stat",
                "gridPos": {"h": 8, "w": 6, "x": 18, "y": 8},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(celery_tasks_in_progress)",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 100},
                                {"color": "red", "value": 200},
                            ]
                        },
                    }
                },
                "options": {"colorMode": "background", "graphMode": "area"},
            },
            # Panel 11: Celery Broker Pool Usage
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 11,
                "title": "🔌 Celery Broker Pool Usage",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 40},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "celery_broker_pool_size",
                        "legendFormat": "Pool Size (Total)",
                        "refId": "A",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "celery_broker_pool_in_use",
                        "legendFormat": "Connections In Use",
                        "refId": "B",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "celery_broker_pool_available",
                        "legendFormat": "Connections Available",
                        "refId": "C",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "axisLabel": "Connections",
                            "fillOpacity": 15,
                            "lineWidth": 2,
                            "drawStyle": "line",
                        },
                    },
                    "overrides": [
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Connections In Use",
                            },
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "orange", "mode": "fixed"},
                                }
                            ],
                        },
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Connections Available",
                            },
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "green", "mode": "fixed"},
                                }
                            ],
                        },
                    ],
                },
            },
            # Panel 12: Broker Pool Saturation Gauge
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 12,
                "title": "Broker Pool Saturation",
                "type": "gauge",
                "gridPos": {"h": 8, "w": 6, "x": 12, "y": 40},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "celery_broker_pool_in_use / celery_broker_pool_size * 100",
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
                                {"color": "green", "value": 0},
                                {"color": "yellow", "value": 50},
                                {"color": "orange", "value": 80},
                                {"color": "red", "value": 95},
                            ],
                        },
                    }
                },
                "options": {
                    "showThresholdLabels": True,
                    "showThresholdMarkers": True,
                },
            },
            # Panel 13: Pool Availability Stat
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 13,
                "title": "Available Pool Connections",
                "type": "stat",
                "gridPos": {"h": 8, "w": 6, "x": 18, "y": 40},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "celery_broker_pool_available",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {
                            "steps": [
                                {"color": "red", "value": 0},
                                {"color": "yellow", "value": 2},
                                {"color": "green", "value": 5},
                            ]
                        },
                    }
                },
                "options": {"colorMode": "background", "graphMode": "area"},
            },
            # NEW Panel 7: Task Duration (P50, P95, P99)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 7,
                "title": "Task Duration Percentiles",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 16},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "histogram_quantile(0.50, sum(rate(celery_task_duration_seconds_bucket[1m])) by (le))",
                        "legendFormat": "P50 (median)",
                        "refId": "A",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "histogram_quantile(0.95, sum(rate(celery_task_duration_seconds_bucket[1m])) by (le))",
                        "legendFormat": "P95",
                        "refId": "B",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "histogram_quantile(0.99, sum(rate(celery_task_duration_seconds_bucket[1m])) by (le))",
                        "legendFormat": "P99",
                        "refId": "C",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "s",
                        "custom": {
                            "axisLabel": "Duration (seconds)",
                            "fillOpacity": 10,
                            "lineWidth": 2,
                        },
                    }
                },
            },
            # NEW Panel 8: Success Rate %
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 8,
                "title": "Task Success Rate",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 16},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(rate(celery_task_total{status='success'}[1m])) / sum(rate(celery_task_total[1m])) * 100",
                        "legendFormat": "Success Rate %",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "percent",
                        "min": 0,
                        "max": 100,
                        "custom": {
                            "axisLabel": "Success %",
                            "fillOpacity": 20,
                            "lineWidth": 2,
                        },
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "red", "value": None},
                                {"color": "yellow", "value": 80},
                                {"color": "green", "value": 95},
                            ],
                        },
                    }
                },
            },
            # Panel 9: THE KEY CORRELATION - Connections vs Task Throughput
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 9,
                "title": "🔍 CRITICAL: Connection Usage vs Task Throughput",
                "type": "timeseries",
                "gridPos": {"h": 10, "w": 24, "x": 0, "y": 24},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "redis_connected_clients / redis_config_maxclients * 100",
                        "legendFormat": "Connection Usage %",
                        "refId": "A",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(rate(celery_task_total[30s])) * 5",
                        "legendFormat": "Task Throughput (scaled 5x for visibility)",
                        "refId": "B",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "fillOpacity": 15,
                            "lineWidth": 3,
                            "drawStyle": "line",
                            "lineInterpolation": "smooth",
                        },
                    },
                    "overrides": [
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Task Throughput (scaled 5x for visibility)",
                            },
                            "properties": [
                                {"id": "custom.axisPlacement", "value": "right"},
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "blue", "mode": "fixed"},
                                },
                            ],
                        },
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Connection Usage %",
                            },
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "orange", "mode": "fixed"},
                                }
                            ],
                        },
                    ],
                },
                "options": {
                    "legend": {
                        "displayMode": "list",
                        "placement": "bottom",
                        "showLegend": True,
                        "calcs": ["mean", "lastNotNull"],
                    }
                },
            },
            # Panel 10: Summary Stats Table
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 10,
                "title": "Summary Statistics",
                "type": "table",
                "gridPos": {"h": 6, "w": 24, "x": 0, "y": 34},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(celery_task_total)",
                        "legendFormat": "Total Tasks",
                        "refId": "A",
                        "instant": True,
                        "format": "table",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(celery_task_total{status='success'})",
                        "legendFormat": "Successful",
                        "refId": "B",
                        "instant": True,
                        "format": "table",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(celery_task_total{status='failure'})",
                        "legendFormat": "Failed",
                        "refId": "C",
                        "instant": True,
                        "format": "table",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(rate(celery_task_total[5m])) * 60",
                        "legendFormat": "Avg Rate (tasks/min)",
                        "refId": "D",
                        "instant": True,
                        "format": "table",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "redis_connected_clients",
                        "legendFormat": "Current Connections",
                        "refId": "E",
                        "instant": True,
                        "format": "table",
                    },
                ],
                "transformations": [{"id": "merge", "options": {}}],
            },
        ],
    }

    return dashboard


def save_dashboard(dashboard, filename="lab01-enhanced-connection-exhaustion.json"):
    """Save dashboard to file"""
    target_dir = Path("monitoring/dashboards")
    target_dir.mkdir(parents=True, exist_ok=True)

    filepath = target_dir / filename

    # Write the dashboard directly (no wrapper needed for file provisioning)
    with filepath.open("w") as f:
        json.dump(dashboard, f, indent=2)

    print(f"✓ Dashboard saved to: {filepath}")
    return filepath


def main():
    print("Generating Enhanced Dashboard with Task Metrics...")
    print("-" * 60)

    dashboard = create_connection_exhaustion_dashboard()
    _filepath = save_dashboard(dashboard)

    print("\nDashboard includes:")
    print("  ✓ Connection pool metrics")
    print("  ✓ Task throughput (tasks/sec)")
    print("  ✓ Task duration percentiles")
    print("  ✓ Success rate tracking")
    print("  ✓ Correlation view (connections vs throughput)")
    print("\nNext steps:")
    print("  1. Run this script: python monitoring/generate_dashboards.py")
    print("  2. If using file provisioning, restart Grafana.")
    print("  3. Open: http://localhost:3000/d/lab01-connection-exhaustion-enhanced")


if __name__ == "__main__":
    main()
