# BOOM on NRP Nautilus

This repository contains the Kubernetes deployment manifests and technical documentation for running the BOOM (Burst & Outburst Observations Monitor) microservice stack on the [NRP Nautilus](https://nrp-nautilus.io/) cluster.

## Deployment Overview

The stack is deployed in the `umn-babamul` namespace. It provides a production-grade environment for transient astronomy ingestion and analysis.

- **API Endpoint:** `https://boom-api.nrp-nautilus.io`
- **Current Data Status:** April 8 and 9, 2026 ZTF public alerts are fully ingested and available.
- **Components:** Kafka (KRaft), MongoDB, Valkey, Prometheus/Grafana, and the BOOM Rust microservices.

## Getting Started

1. **Namespace Setup:** Ensure you are in the `umn-babamul` namespace.
2. **Secrets:** Apply `k8s/secrets.yaml` (after updating it with your own credentials).
3. **Infrastructure:** Apply these core services first:
   ```bash
   kubectl apply -f k8s/configmaps.yaml
   kubectl apply -f k8s/mongodb.yaml
   kubectl apply -f k8s/valkey.yaml
   kubectl apply -f k8s/kafka.yaml
   ```
4. **App Services:** Once infrastructure is ready, apply the BOOM microservices:
   ```bash
   kubectl apply -f k8s/boom-api.yaml
   kubectl apply -f k8s/boom-consumer-ztf.yaml
   kubectl apply -f k8s/boom-scheduler-ztf.yaml
   ```
5. **Observability:** Apply the monitoring stack:
   ```bash
   kubectl apply -f k8s/exporters.yaml
   kubectl apply -f k8s/otel-collector.yaml
   kubectl apply -f k8s/prometheus.yaml
   kubectl apply -f k8s/grafana.yaml
   ```
6. **Validation:** Check that all pods are running in the `umn-babamul` namespace.

## Key Documentation

- [Deployment Notes & Troubleshooting](DEPLOYMENT_NOTES.md): Critical information on solving storage latency and Kafka race conditions on NRP.
- [Data Ingestion Guide](INGESTION.md): How to load additional nights of ZTF or DECam data.
