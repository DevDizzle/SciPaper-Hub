# Infrastructure Decommissioning and Recommissioning Instructions

This document provides instructions for decommissioning and recommissioning the infrastructure for the SciPaper-Hub project.

## Decommissioning

To decommission the infrastructure, you need to delete the following resources in Google Cloud Platform:

1.  **Vertex AI Index Endpoint:** This will undeploy the index and stop incurring costs for the endpoint.
    *   Navigate to the Vertex AI > Vector Search > Index Endpoints in the Google Cloud Console.
    *   Select the index endpoint used for this project.
    *   Undeploy all deployed indexes from the endpoint.
    *   Delete the index endpoint.

2.  **Vertex AI Index:** This will delete the index and all the data it contains.
    *   Navigate to the Vertex AI > Vector Search > Indexes in the Google Cloud Console.
    *   Select the index used for this project.
    *   Delete the index.

3.  **Google Cloud Storage Bucket:** This will delete the bucket and all the data it contains.
    *   Navigate to the Cloud Storage > Buckets in the Google Cloud Console.
    *   Select the bucket used for this project.
    *   Delete the bucket.

4.  **Cloud Run/App Engine Service:** This will delete the service running the FastAPI application.
    *   Navigate to the Cloud Run or App Engine section in the Google Cloud Console.
    *   Select the service used for this project.
    *   Delete the service.

**Note:** The names of the resources (index, endpoint, bucket, service) can be found in the environment variables or configuration files of the project.

## Recommissioning

To recommission the infrastructure, you need to create the following resources in Google Cloud Platform:

1.  **Google Cloud Storage Bucket:**
    *   Create a new Cloud Storage bucket.
    *   Upload the Parquet files to this bucket.

2.  **Vertex AI Index:**
    *   Create a new Vertex AI Index.
    *   Configure the index with the appropriate dimensions for the embeddings (e.g., 768 for `gemini-embedding-001`).
    *   Use the `pipelines/indexer.py` script to populate the index from the Parquet files in the Cloud Storage bucket.

3.  **Vertex AI Index Endpoint:**
    *   Create a new Vertex AI Index Endpoint.
    *   Deploy the index to this endpoint.

4.  **Cloud Run/App Engine Service:**
    *   Build and deploy the Docker image to Cloud Run or App Engine.
    *   Set the following environment variables:
        *   `INDEX_ENDPOINT_ID`: The ID of the Vertex AI Index Endpoint.
        *   `DEPLOYED_INDEX_ID`: The ID of the deployed index on the endpoint.
        *   `GCP_PROJECT`: The Google Cloud project ID.
        *   `GCP_REGION`: The Google Cloud region.
        *   `VERTEX_LOCATION`: The location of the Vertex AI resources.
