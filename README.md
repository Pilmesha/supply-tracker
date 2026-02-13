# Supply Tracker

## Overview

**Supply Tracker** is an automation platform designed to eliminate manual logistics data entry and centralize order-related information across the procurement lifecycle. The system integrates with Zoho One, email services, and shared data repositories to ensure real-time synchronization, improved operational visibility, and reduced human error.

The solution is built for the logistics team at **Vortex Water Engineering** and supports production-grade workflows through event-driven architecture, retry mechanisms, containerization, and cloud deployment.

---

## Intended Audience

This documentation is intended for:

* Software engineers maintaining or extending the system
* DevOps engineers responsible for infrastructure
* Technical managers overseeing logistics automation
* Future contributors onboarding to the project

---

## Key Capabilities

* Automated Purchase Order lifecycle tracking
* Real-time webhook processing from Zoho
* Intelligent email parsing for confirmations and packing lists
* Centralized Excel-based operational datastore
* Automated customer notifications
* Delivery date prediction
* Parallel processing for improved throughput
* Resilient file handling with retry logic

---

## High-Level Architecture

```
Zoho One ──► Webhooks ──► API Service (Render + Docker)
                               │
                               ▼
                      Processing Layer
        (Validation, Parsing, Business Rules, Matching)
                               │
              ┌────────────────┴───────────────┐
              ▼                                ▼
        Excel Datastore                Email Notifications
        (SharePoint / Cloud)           (Customers)
```

The system follows an **event-driven architecture**, where external triggers initiate deterministic processing pipelines.

---

## Tech Stack

**Backend:** Python
**API Layer:** REST-based service
**External Integrations:**

* Zoho API
* Microsoft Graph (email subscriptions)

**Infrastructure:**

* Docker (containerization)
* Render (cloud hosting)
* Fastcron (scheduled jobs)

**Storage Strategy:** Excel-based operational datastore located in a shared environment.

---

## System Components

### Data Sources

#### Zoho One

Triggers system workflows when the following events occur:

* Purchase Order created / approved
* Package delivered
* Purchase received
* Invoice paid

Webhooks automatically push event payloads to the API.

---

#### Email Service

The system monitors designated inboxes and processes attachments such as:

* Order Confirmations
* Packing Lists

Documents are parsed automatically to extract structured data.

---

#### Shared Data Repository

Provides enrichment and validation datasets:

* HS codes
* Product translations
* Regulatory flags (e.g., reagents)
* Letter requirements

---

## API Architecture

### Zoho Integration Endpoints

* `/purchase`
* `/receive`
* `/delivered`
* `/invoice`

### Email Integration

* `/webhook`

### Internal Service Endpoints

* `/init` — renews email subscriptions
* `/subscriptions` — manages active subscriptions
* `/cleanup` — removes expired subscriptions

Because email subscriptions expire every **48 hours**, Fastcron triggers `/init` periodically to ensure uninterrupted monitoring.

---

## Core Processing Flows

### 1. Purchase Order Processing

When a Purchase Order becomes confirmed:

1. Webhook sends metadata to `/purchase`.
2. The system retrieves the related Sales Order.
3. Items are matched via SKU.
4. Delivery metadata is calculated.
5. Supplier classification determines the processing path.

**HACH suppliers:**

* A dedicated sheet is created per PO.
* Regulatory data, HS codes, and translations are injected.

**Non-HACH suppliers:**

* Records are appended to the secondary PO status workbook.
* Visual tagging is applied for quick operational scanning.

---

### 2. Purchase Received

Triggered when inventory is marked as received.

* Items are separated by supplier type.
* Delivery dates are adjusted.
* Quantities are reconciled against the PO.
* Workbooks are updated automatically.

---

### 3. Package Delivered

Ensures items are only marked delivered when **fully received**.

Process:

1. Aggregate quantities across package IDs.
2. Compare with ordered quantities.
3. Move completed items to the delivered sheet.
4. Update location to "Delivered".
5. Notify customers via automated email.

Notification type depends on the customer profile:

* Selected enterprise customers → acceptance documentation requested.
* Others → payment reminder.

Packing PDFs are generated and attached automatically.

---

### 4. Invoice Paid

When payment is confirmed:

* Lead-time text is parsed.
* Delivery windows are calculated (including ranges).
* Matching PO entries are updated with projected delivery dates.

---

### 5. Mail Processing Engine

The system classifies incoming messages into predefined categories using sender and subject heuristics.

Supported patterns include:

* Generic confirmations
* HACH confirmations
* HACH packing lists
* Khrone confirmations
* Khrone packing lists

Attachments are parsed, matched to PO records, and reflected in the datastore.

---

## Business Rules (Examples)

* Orders shipping to Armenia or Azerbaijan are flagged as exports.
* Items are marked delivered only when cumulative quantities match the original order.
* Regulatory letters are required when products are classified as reagents.

Separating these rules ensures they can evolve without altering architectural logic.

---

## Reliability & Error Handling

The system prioritizes operational resilience:

* Centralized logging captures both actions and failures.
* File-lock scenarios trigger exponential backoff retries (up to 30 seconds).
* Parallel execution prevents independent workflows from blocking each other.
* Reusable HTTP sessions reduce latency and connection overhead.
* SQLite database for storing already processed emails to prevent processing the same email multiple times when it is sent to several tracked recipients.
---

## Security

* Zoho endpoints are protected via unique verification codes.
* Sensitive configuration is stored in environment variables.
* API keys are never hardcoded.

---

## Deployment

The platform runs on **Render** within a **Dockerized environment**, ensuring consistent runtime behavior across infrastructures.

Benefits include:

* Environment standardization
* Simplified deployments
* Infrastructure portability

To prevent cold starts associated with free-tier hosting, Fastcron pings the `/health` endpoint every 15 minutes to keep the service active.

---

## System Limitations

* Excel is not ideal for high-scale transactional workloads.
* Free-tier infrastructure may introduce latency.
* Email parsing depends on supplier formatting consistency.

These constraints should be considered in future scalability planning.

---

## Future Improvements (Recommended)

* Migrate datastore to a relational database.
* Introduce message queues for higher throughput.
* Implement observability tooling (metrics + alerting).
* Add role-based access to the web interface.

---

## Author

**Nia Nozadze**
GitHub: [https://github.com/Pilmesha](https://github.com/Pilmesha)

Built for the Logistics Team at **Vortex Water Engineering** — 2026
