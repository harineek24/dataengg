# Deployment Guide

Three ways to deploy this portfolio app — pick whichever fits your workflow.

---

## Option 1: Streamlit Community Cloud (Free — Recommended for Portfolio)

The easiest option. Free hosting, auto-deploys from GitHub, gives you a public URL.

### Steps

1. Push this repo to GitHub (public or private).

2. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with GitHub.

3. Click **"New app"** and fill in:
   - **Repository:** `harineek24/dataengg`
   - **Branch:** `main`
   - **Main file path:** `app.py`

4. Click **Deploy**. Your app will be live at:
   ```
   https://harineek24-dataengg.streamlit.app
   ```

5. Every push to `main` auto-redeploys.

---

## Option 2: Cloud Run (GCP-Native, Containerized)

Good if you want to keep everything on GCP and show Cloud Run skills.

### Steps

```bash
# Set variables
export PROJECT_ID=$(gcloud config get-value project)
export REGION="us-central1"

# Enable APIs
gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com

# Create Artifact Registry repo (one-time)
gcloud artifacts repositories create streamlit-apps \
    --repository-format=docker \
    --location=${REGION} \
    --quiet

# Build and push the container
export IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/streamlit-apps/portfolio:latest"
gcloud builds submit --tag ${IMAGE_URI} .

# Deploy to Cloud Run
gcloud run deploy gcp-de-portfolio \
    --image=${IMAGE_URI} \
    --region=${REGION} \
    --platform=managed \
    --allow-unauthenticated \
    --memory=512Mi \
    --cpu=1 \
    --port=8501

# Get the URL
gcloud run services describe gcp-de-portfolio \
    --region=${REGION} --format="value(status.url)"
```

### Estimated cost
- **Free tier:** 2 million requests/month, 360k vCPU-seconds/month
- A portfolio site will almost certainly stay within free tier

---

## Option 3: Run Locally

For local development and testing.

### Steps

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run app.py
```

Opens at `http://localhost:8501`.

---

## After Deploying

- Share the URL on your **LinkedIn**, **GitHub profile**, and **resume**
- Add a screenshot of the app to your GitHub README for visibility
