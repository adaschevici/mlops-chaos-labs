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
        "tags": ["chaos-lab", "redis", "celery", "connections"],
        "timezone": "browser",
        "schemaVersion": 38,
        "version": 1,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "timepicker": {},
        "fiscalYearStartMonth": 0,
        "panels": [
            # ============================================================
            # SCENARIO 1: APPLICATION-LEVEL REDIS CONNECTION EXHAUSTION
            # ============================================================
            {
                "type": "row",
                "id": 1,
                "title": "🔴 SCENARIO 1: Redis Connection Pool Exhaustion (Application-Level)",
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": 0},
                "collapsed": False,
            },
            # Panel 1: Connection Pool Usage
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 2,
                "title": "Redis Connection Pool Usage",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 1},
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
                "overrides": [
                    {
                        "matcher": {"id": "byName", "options": "Max Clients Limit"},
                        "properties": [
                            {
                                "id": "custom.lineStyle",
                                "value": {"dash": [10, 10], "fill": "dash"},
                            },
                            {
                                "id": "color",
                                "value": {"fixedColor": "red", "mode": "fixed"},
                            },
                        ],
                    }
                ],
            },
            # Panel 2: Usage % Gauge
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 3,
                "title": "Connection Pool Usage %",
                "type": "gauge",
                "gridPos": {"h": 8, "w": 6, "x": 12, "y": 1},
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
            # Panel 3: Rejected Connections
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 4,
                "title": "Rejected Connections",
                "type": "stat",
                "gridPos": {"h": 8, "w": 6, "x": 18, "y": 1},
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
            # Panel 4: Task Throughput (Tasks/sec)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 5,
                "title": "⚡ Task Throughput (Tasks/sec)",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 9},
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
            # Panel 5: Current Task Rate Gauge
            # ------------------------------------------------------------------
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 6,
                "title": "Current Task Rate",
                "type": "gauge",
                "gridPos": {"h": 8, "w": 6, "x": 12, "y": 9},
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
                        "max": 650,  # Increased to accommodate 118+ ops/s
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
            # Panel 6: Tasks In Progress
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 7,
                "title": "Tasks In Progress",
                "type": "stat",
                "gridPos": {"h": 8, "w": 6, "x": 18, "y": 9},
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
            # Panel 7: Task Duration (P50, P95, P99)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 8,
                "title": "Task Duration Percentiles",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 17},
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
            # Panel 8: Success Rate %
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 9,
                "title": "Task Success Rate",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 17},
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
                "id": 10,
                "title": "🔍 CRITICAL: Connection Usage vs Task Throughput",
                "type": "timeseries",
                "gridPos": {"h": 10, "w": 24, "x": 0, "y": 25},
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
                        "displayMode": "table",
                        "placement": "bottom",
                        "showLegend": True,
                        "calcs": ["mean", "max", "lastNotNull"],
                    }
                },
            },
            # Better approach: Summary stats as stat panels in a row
            {
                "type": "row",
                "id": 11,
                "title": "📊 Summary Statistics",
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": 35},
                "collapsed": False,
            },
            # Total Tasks
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 12,
                "title": "Total Tasks",
                "type": "stat",
                "gridPos": {"h": 4, "w": 5, "x": 0, "y": 36},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(celery_task_total)",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {"steps": [{"color": "blue", "value": None}]},
                        "decimals": 0,
                    }
                },
                "options": {
                    "colorMode": "background",
                    "graphMode": "none",
                    "textMode": "value_and_name",
                },
            },
            # Successful Tasks
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 13,
                "title": "Successful Tasks",
                "type": "stat",
                "gridPos": {"h": 4, "w": 5, "x": 5, "y": 36},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(celery_task_total{status='success'})",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {"steps": [{"color": "green", "value": None}]},
                        "decimals": 0,
                    }
                },
                "options": {
                    "colorMode": "background",
                    "graphMode": "none",
                    "textMode": "value_and_name",
                },
            },
            # Failed Tasks
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 14,
                "title": "Failed Tasks",
                "type": "stat",
                "gridPos": {"h": 4, "w": 5, "x": 10, "y": 36},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(celery_task_total{status='failure'})",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {"steps": [{"color": "red", "value": None}]},
                        "decimals": 0,
                    }
                },
                "options": {
                    "colorMode": "background",
                    "graphMode": "none",
                    "textMode": "value_and_name",
                },
            },
            # Average Rate
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 15,
                "title": "Avg Rate (tasks/min)",
                "type": "stat",
                "gridPos": {"h": 4, "w": 5, "x": 15, "y": 36},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(rate(celery_task_total[5m])) * 60",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {"steps": [{"color": "yellow", "value": None}]},
                        "decimals": 2,
                        "unit": "cpm",
                    }
                },
                "options": {
                    "colorMode": "background",
                    "graphMode": "area",
                    "textMode": "value_and_name",
                },
            },
            # Current Connections
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 16,
                "title": "Redis Connections",
                "type": "stat",
                "gridPos": {"h": 4, "w": 4, "x": 20, "y": 36},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "redis_connected_clients",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 50},
                                {"color": "red", "value": 90},
                            ]
                        },
                        "decimals": 0,
                    }
                },
                "options": {
                    "colorMode": "background",
                    "graphMode": "area",
                    "textMode": "value_and_name",
                },
            },
            # ============================================================
            # SCENARIO 2: CELERY BROKER POOL CONTENTION
            # ============================================================
            {
                "type": "row",
                "id": 17,
                "title": "🔶 SCENARIO 2: Celery Broker Pool Contention (Application-Level)",
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": 41},
                "collapsed": False,
            },
            # Panel 11: Redis Broker Pool Usage
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 18,
                "title": "🔌 Redis Broker Pool (Celery-Managed)",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 42},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(redis_pool_size{pool_type='broker'})",
                        "legendFormat": "Pool Max Size",
                        "refId": "A",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(redis_pool_in_use{pool_type='broker'})",
                        "legendFormat": "Connections In Use",
                        "refId": "B",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(redis_pool_available{pool_type='broker'})",
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
                                },
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
                                },
                            ],
                        },
                    ],
                },
            },
            # Panel 12: Broker Pool Saturation (FIXED QUERY)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 19,
                "title": "Broker Pool Saturation",
                "type": "gauge",
                "gridPos": {"h": 8, "w": 6, "x": 12, "y": 42},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        # FIX: Use sum() to aggregate across workers
                        # "expr": "sum(redis_pool_in_use{pool_type='broker'}) / sum(redis_pool_size{pool_type='broker'}) * 100",
                        "expr": "avg(celery_broker_pool_saturation_percent)",
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
                "options": {"showThresholdLabels": True, "showThresholdMarkers": True},
            },
            # Panel 13: Queue Depth
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 20,
                "title": "Task Queue Depth",
                "type": "stat",
                "gridPos": {"h": 8, "w": 6, "x": 18, "y": 42},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "max(celery_task_queue_depth{queue_name='celery'})",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": 0},
                                {"color": "yellow", "value": 100},
                                {"color": "red", "value": 500},
                            ]
                        },
                    }
                },
                "options": {"colorMode": "background", "graphMode": "area"},
            },
            # Panel 14: Task Publish Duration (Shows Broker Pool Contention)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 21,
                "title": "⏱️ Task Publish Duration (Broker Pool Contention Indicator)",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 50},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "histogram_quantile(0.50, sum(rate(celery_publish_duration_seconds_bucket[1m])) by (le))",
                        "legendFormat": "P50 (median)",
                        "refId": "A",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "histogram_quantile(0.95, sum(rate(celery_publish_duration_seconds_bucket[1m])) by (le))",
                        "legendFormat": "P95",
                        "refId": "B",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "histogram_quantile(0.99, sum(rate(celery_publish_duration_seconds_bucket[1m])) by (le))",
                        "legendFormat": "P99",
                        "refId": "C",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "s",
                        "custom": {
                            "axisLabel": "Publish Duration (seconds)",
                            "fillOpacity": 15,
                            "lineWidth": 2,
                        },
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 0.1},  # >100ms = warning
                                {"color": "orange", "value": 0.5},  # >500ms = problem
                                {"color": "red", "value": 1.0},  # >1s = critical
                            ],
                        },
                    },
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "P99"},
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "red", "mode": "fixed"},
                                },
                            ],
                        },
                    ],
                },
            },
            # Panel 15: Publish Rate
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 22,
                "title": "Task Publish Rate",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 6, "x": 12, "y": 50},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(rate(celery_publish_total[30s]))",
                        "legendFormat": "Publishes/sec",
                        "refId": "A",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "ops",
                        "custom": {
                            "axisLabel": "Publishes/second",
                            "fillOpacity": 20,
                            "lineWidth": 2,
                        },
                    },
                },
            },
            # Panel 16: Current Publish Duration Gauge
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 23,
                "title": "Current P95 Publish Time",
                "type": "gauge",
                "gridPos": {"h": 8, "w": 6, "x": 18, "y": 50},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "histogram_quantile(0.95, sum(rate(celery_publish_duration_seconds_bucket[1m])) by (le))",
                        "refId": "A",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "s",
                        "min": 0,
                        "max": 10,
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": 0},
                                {"color": "yellow", "value": 0.1},
                                {"color": "orange", "value": 0.5},
                                {"color": "red", "value": 1.0},
                            ],
                        },
                    }
                },
                "options": {"showThresholdLabels": True, "showThresholdMarkers": True},
            },
            # Panel 17: Queue Depth vs Publish Duration (THE KEY CORRELATION)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 24,
                "title": "🔍 SMOKING GUN: Queue Backup vs Publish Slowdown",
                "type": "timeseries",
                "gridPos": {"h": 10, "w": 24, "x": 0, "y": 58},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "max(celery_task_queue_depth{queue_name='celery'})",
                        "legendFormat": "Queue Depth (tasks)",
                        "refId": "A",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "histogram_quantile(0.95, sum(rate(celery_publish_duration_seconds_bucket[1m])) by (le)) * 1000",
                        "legendFormat": "Publish P95 Duration (ms)",
                        "refId": "B",
                    },
                    {
                        "expr": "sum(celery_concurrent_publishes)",
                        "legendFormat": "Concurrent Publishes",
                        "refId": "C",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "fillOpacity": 20,
                            "lineWidth": 3,
                            "drawStyle": "line",
                            "lineInterpolation": "smooth",
                        },
                    },
                    "overrides": [
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Queue Depth (tasks)",
                            },
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "red", "mode": "fixed"},
                                },
                                {"id": "custom.axisPlacement", "value": "left"},
                                {"id": "unit", "value": "short"},
                            ],
                        },
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Publish P95 Duration (ms)",
                            },
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "orange", "mode": "fixed"},
                                },
                                {"id": "custom.axisPlacement", "value": "right"},
                                {"id": "unit", "value": "ms"},
                            ],
                        },
                    ],
                },
                "options": {
                    "legend": {
                        "displayMode": "table",
                        "placement": "bottom",
                        "showLegend": True,
                        "calcs": ["mean", "max", "lastNotNull"],
                    }
                },
            },
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 25,
                "title": "🔥 Concurrent Publishes & Publish Queue",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 57},
                "targets": [
                    {
                        "expr": "sum(celery_concurrent_publishes)",
                        "legendFormat": "Concurrent Publishes",
                        "refId": "A",
                    },
                    {
                        "expr": "sum(celery_publish_queue_depth)",
                        "legendFormat": "Waiting for Pool",
                        "refId": "B",
                    },
                    {
                        "expr": "sum(celery_broker_pool_max)",
                        "legendFormat": "Broker Pool Limit",
                        "refId": "C",
                    },
                ],
                "fieldConfig": {
                    "defaults": {"custom": {"fillOpacity": 15, "lineWidth": 2}},
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "Broker Pool Limit"},
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "red", "mode": "fixed"},
                                },
                                {
                                    "id": "custom.lineStyle",
                                    "value": {"dash": [10, 10], "fill": "dash"},
                                },
                                {"id": "custom.fillOpacity", "value": 0},
                            ],
                        },
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Concurrent Publishes",
                            },
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "orange", "mode": "fixed"},
                                }
                            ],
                        },
                        {
                            "matcher": {"id": "byName", "options": "Waiting for Pool"},
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "purple", "mode": "fixed"},
                                }
                            ],
                        },
                    ],
                },
            },
            # Panel 18: Publish Success vs Failures
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 26,
                "title": "📤 Publish Success vs Failures",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 66},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(rate(celery_publish_total[30s]))",
                        "legendFormat": "Successful Publishes/sec",
                        "refId": "A",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(rate(celery_publish_failed_total[30s]))",
                        "legendFormat": "Failed Publishes/sec",
                        "refId": "B",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "sum(rate(celery_publish_connection_errors_total[30s]))",
                        "legendFormat": "Connection Errors/sec",
                        "refId": "C",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "fillOpacity": 20,
                            "lineWidth": 2,
                        },
                    },
                    "overrides": [
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Failed Publishes/sec",
                            },
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "red", "mode": "fixed"},
                                },
                            ],
                        },
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Connection Errors/sec",
                            },
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {
                                        "fixedColor": "dark-red",
                                        "mode": "fixed",
                                    },
                                },
                            ],
                        },
                    ],
                },
            },
            # Panel 19: Task Duration (P50, P95, P99)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 27,
                "title": "Task Duration Percentiles",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 6, "x": 12, "y": 66},
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
            # Panel 20: Broker Pool Pressure Indicator (MOST USEFUL)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 28,
                "title": "🔥 Broker Pool Pressure",
                "type": "gauge",
                "gridPos": {"h": 8, "w": 6, "x": 18, "y": 66},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        # Shows how long publishes are taking (higher = more contention)
                        "expr": "histogram_quantile(0.95, sum(rate(celery_publish_duration_seconds_bucket[1m])) by (le)) * 1000",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "ms",
                        "min": 0,
                        "max": 1000,  # 1 second
                        "decimals": 0,
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": 0},  # 0-10ms: Normal
                                {
                                    "color": "yellow",
                                    "value": 10,
                                },  # 10-50ms: Slight pressure
                                {
                                    "color": "orange",
                                    "value": 50,
                                },  # 50-100ms: Moderate pressure
                                {"color": "red", "value": 100},  # 100ms+: High pressure
                            ],
                        },
                    }
                },
                "options": {
                    "showThresholdLabels": True,
                    "showThresholdMarkers": True,
                    "text": {"titleSize": 14, "valueSize": 32},
                },
            },
            # Panel 21: Result Get Duration (Shows Blocking)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 29,
                "title": "⏱️ Result Get Duration (Pool Blocking Indicator)",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 74},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "histogram_quantile(0.50, sum(rate(celery_result_get_duration_seconds_bucket{status='success'}[1m])) by (le))",
                        "legendFormat": "P50 (successful)",
                        "refId": "A",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "histogram_quantile(0.95, sum(rate(celery_result_get_duration_seconds_bucket{status='success'}[1m])) by (le))",
                        "legendFormat": "P95 (successful)",
                        "refId": "B",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "histogram_quantile(0.50, sum(rate(celery_result_get_duration_seconds_bucket{status='timeout'}[1m])) by (le))",
                        "legendFormat": "P50 (timeout)",
                        "refId": "C",
                    },
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "s",
                        "custom": {
                            "axisLabel": "Get Duration (seconds)",
                            "fillOpacity": 15,
                            "lineWidth": 2,
                        },
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 5},
                                {"color": "orange", "value": 20},
                                {"color": "red", "value": 45},
                            ],
                        },
                    },
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "P50 (timeout)"},
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "red", "mode": "fixed"},
                                },
                            ],
                        },
                    ],
                },
            },
            # ============================================================
            # SCENARIO 2 SUMMARY STATISTICS (FIXED LAYOUT)
            # ============================================================
            {
                "type": "row",
                "id": 30,
                "title": "📊 Scenario 2 Summary - Broker Pool Contention Impact",
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": 74},
                "collapsed": False,
            },
            # Updated Panel for Broker Pool Saturation in Scenario 2 Summary
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 31,
                "title": "Broker Pool Saturation",
                "type": "stat",
                "gridPos": {"h": 5, "w": 4, "x": 0, "y": 75},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        # Use the new direct saturation metric
                        "expr": "celery_broker_pool_saturation_percent",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": 0},  # 0-50%: Healthy
                                {"color": "yellow", "value": 50},  # 50-80%: Warning
                                {
                                    "color": "orange",
                                    "value": 80,
                                },  # 80-95%: High pressure
                                {"color": "red", "value": 95},  # 95%+: Saturated
                            ]
                        },
                        "unit": "percent",
                        "decimals": 1,
                    }
                },
                "options": {
                    "colorMode": "background",
                    "graphMode": "area",
                    "textMode": "value_and_name",
                    "reduceOptions": {"values": False, "calcs": ["lastNotNull"]},
                },
            },
            # Also add a detailed broker pool panel in the main scenario section
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 32,
                "title": "🔌 Celery Broker Pool Utilization",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 50},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "celery_broker_pool_max",
                        "legendFormat": "Pool Max Size (Config Limit)",
                        "refId": "A",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "celery_broker_pool_active",
                        "legendFormat": "Active Connections (In Use)",
                        "refId": "B",
                    },
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "celery_concurrent_publishes",
                        "legendFormat": "Concurrent Publishes",
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
                        },
                    },
                    "overrides": [
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Active Connections (In Use)",
                            },
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "orange", "mode": "fixed"},
                                },
                            ],
                        },
                        {
                            "matcher": {
                                "id": "byName",
                                "options": "Pool Max Size (Config Limit)",
                            },
                            "properties": [
                                {
                                    "id": "color",
                                    "value": {"fixedColor": "red", "mode": "fixed"},
                                },
                                {"id": "custom.fillOpacity", "value": 0},
                                {
                                    "id": "custom.lineStyle",
                                    "value": {"dash": [10, 10], "fill": "dash"},
                                },
                            ],
                        },
                    ],
                },
            },
            # Broker Pool Saturation Gauge (for main section)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 33,
                "title": "Broker Pool Saturation %",
                "type": "gauge",
                "gridPos": {"h": 8, "w": 6, "x": 12, "y": 50},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "celery_broker_pool_saturation_percent",
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
                "options": {"showThresholdLabels": True, "showThresholdMarkers": True},
            },
            # Publish Queue Depth (tasks waiting for broker connection)
            {
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "id": 34,
                "title": "Publish Queue (Waiting for Pool)",
                "type": "stat",
                "gridPos": {"h": 8, "w": 6, "x": 18, "y": 50},
                "targets": [
                    {
                        "datasource": {"type": "prometheus", "uid": "prometheus"},
                        "expr": "celery_publish_queue_depth",
                        "refId": "A",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": 0},
                                {"color": "yellow", "value": 10},
                                {"color": "orange", "value": 50},
                                {"color": "red", "value": 100},
                            ]
                        },
                        "decimals": 0,
                    }
                },
                "options": {
                    "colorMode": "background",
                    "graphMode": "area",
                    "textMode": "value_and_name",
                },
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
