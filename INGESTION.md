# Data Ingestion Guide

This guide explains how to load archival ZTF alerts into the BOOM stack running on NRP.

## Currently Loaded Data
- **2026-04-08** (Public ZTF)
- **2026-04-09** (Public ZTF)

## Loading a New Night

To load a new night, follow these steps:

1. **Update the Manifest:** 
   Modify `k8s/ztf-ingest-job.yaml`. Change the date in the Job name, the `TOPIC` variable in the `initContainer`, and the arguments for the `producer` container.
   
   Example arguments for the producer:
   `args: ["ztf", "20260410", "public", "--server-url", "broker:29092"]`

2. **Apply the Job:**
   ```bash
   kubectl apply -f k8s/ztf-ingest-job.yaml -n umn-babamul
   ```

3. **Monitor Progress:**
   
   **Step A: Check Topic Creation**
   ```bash
   kubectl logs job/ztf-ingest-YYYYMMDD -c create-topic -n umn-babamul
   ```
   
   **Step B: Check Streaming**
   ```bash
   kubectl logs job/ztf-ingest-YYYYMMDD -c producer -n umn-babamul -f
   ```
   *(Note: The progress bar is not visible in K8s logs, but you will see the download and extraction logs.)*

4. **Verify Message Count:**
   ```bash
   kubectl exec broker-0 -n umn-babamul -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:29092 --topic ztf_YYYYMMDD_programid1
   ```

## Troubleshooting

- **"All brokers down":** Restart the ingestion job. This usually means the Kafka broker was restarting when the job began.
- **Out of Disk Space:** The ingestion jobs use an `emptyDir` for extracting tarballs. Ensure the node has enough ephemeral-storage (at least 20Gi per job).
