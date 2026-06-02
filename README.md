# System Design Interview Answers

100 system design questions answered with a consistent structure, Mermaid architecture diagrams, and senior-level depth.

## Answer Structure

Every answer follows this format:

| Section | What it covers |
|---------|---------------|
| **Requirements** | Functional (what the system does) + Non-Functional (scale, latency, availability) |
| **Scale Estimation** | Back-of-envelope: QPS, storage, bandwidth |
| **High-Level Architecture** | Component diagram showing the main building blocks |
| **Core Components** | Deep-dive into each subsystem |
| **Data Model** | Schema, storage technology choice and why |
| **API Design** | Key endpoints with method, path, request/response |
| **Key Challenges & Solutions** | Bottlenecks, failure modes, and how to address them |
| **Trade-offs** | CAP position, consistency vs. availability decisions, alternatives considered |

---

## Question Index

### 🔴 Tier 1 — Asked Almost Everywhere (1–15)

| # | Topic | File |
|---|-------|------|
| 1 | URL Shortener (bit.ly) | [tier1/01-url-shortener.md](tier1/01-url-shortener.md) |
| 2 | Rate Limiter | [tier1/02-rate-limiter.md](tier1/02-rate-limiter.md) |
| 3 | Key-Value Store | [tier1/03-key-value-store.md](tier1/03-key-value-store.md) |
| 4 | News Feed (Twitter/Facebook) | [tier1/04-news-feed.md](tier1/04-news-feed.md) |
| 5 | Chat System (WhatsApp/Messenger) | [tier1/05-chat-system.md](tier1/05-chat-system.md) |
| 6 | Notification System | [tier1/06-notification-system.md](tier1/06-notification-system.md) |
| 7 | Web Crawler | [tier1/07-web-crawler.md](tier1/07-web-crawler.md) |
| 8 | Search Autocomplete / Typeahead | [tier1/08-search-autocomplete.md](tier1/08-search-autocomplete.md) |
| 9 | YouTube / Netflix (Video Streaming) | [tier1/09-video-streaming.md](tier1/09-video-streaming.md) |
| 10 | Distributed Cache (Redis-like) | [tier1/10-distributed-cache.md](tier1/10-distributed-cache.md) |
| 11 | Ride-Sharing App (Uber/Lyft) | [tier1/11-ride-sharing.md](tier1/11-ride-sharing.md) |
| 12 | File Storage System (Dropbox/Google Drive) | [tier1/12-file-storage.md](tier1/12-file-storage.md) |
| 13 | Distributed Message Queue (Kafka/RabbitMQ) | [tier1/13-message-queue.md](tier1/13-message-queue.md) |
| 14 | Twitter Search / Full-Text Search | [tier1/14-twitter-search.md](tier1/14-twitter-search.md) |
| 15 | API Gateway | [tier1/15-api-gateway.md](tier1/15-api-gateway.md) |

### 🟠 Tier 2 — Very Frequently Asked (16–35)

| # | Topic | File |
|---|-------|------|
| 16 | Load Balancer | [tier2/16-load-balancer.md](tier2/16-load-balancer.md) |
| 17 | CDN (Content Delivery Network) | [tier2/17-cdn.md](tier2/17-cdn.md) |
| 18 | Distributed ID Generator (Snowflake) | [tier2/18-distributed-id-generator.md](tier2/18-distributed-id-generator.md) |
| 19 | Payment System (Stripe/PayPal) | [tier2/19-payment-system.md](tier2/19-payment-system.md) |
| 20 | E-Commerce Platform (Amazon) | [tier2/20-ecommerce-platform.md](tier2/20-ecommerce-platform.md) |
| 21 | Hotel/Flight Booking System | [tier2/21-booking-system.md](tier2/21-booking-system.md) |
| 22 | Proximity Service / Yelp (Nearby Places) | [tier2/22-proximity-service.md](tier2/22-proximity-service.md) |
| 23 | Leaderboard / Real-time Ranking System | [tier2/23-leaderboard.md](tier2/23-leaderboard.md) |
| 24 | Recommendation Engine (Netflix/Amazon) | [tier2/24-recommendation-engine.md](tier2/24-recommendation-engine.md) |
| 25 | Google Maps / Navigation System | [tier2/25-google-maps.md](tier2/25-google-maps.md) |
| 26 | Distributed Locking System | [tier2/26-distributed-locking.md](tier2/26-distributed-locking.md) |
| 27 | Job Scheduler / Task Queue | [tier2/27-job-scheduler.md](tier2/27-job-scheduler.md) |
| 28 | Log Collection and Monitoring System | [tier2/28-log-monitoring.md](tier2/28-log-monitoring.md) |
| 29 | Metrics and Alerting System (Datadog/Prometheus) | [tier2/29-metrics-alerting.md](tier2/29-metrics-alerting.md) |
| 30 | Pastebin / Code Snippet Sharing | [tier2/30-pastebin.md](tier2/30-pastebin.md) |
| 31 | Stock Exchange / Trading Platform | [tier2/31-stock-exchange.md](tier2/31-stock-exchange.md) |
| 32 | Email System (Gmail) | [tier2/32-email-system.md](tier2/32-email-system.md) |
| 33 | Facebook Live / Live Streaming | [tier2/33-live-streaming.md](tier2/33-live-streaming.md) |
| 34 | Collaborative Document Editor (Google Docs) | [tier2/34-collaborative-editor.md](tier2/34-collaborative-editor.md) |
| 35 | Distributed Search Engine (Google) | [tier2/35-distributed-search.md](tier2/35-distributed-search.md) |

### 🟡 Tier 3 — Commonly Asked at Senior/Staff Levels (36–60)

| # | Topic | File |
|---|-------|------|
| 36 | Pub/Sub System | [tier3/36-pub-sub.md](tier3/36-pub-sub.md) |
| 37 | Real-Time Analytics Dashboard | [tier3/37-realtime-analytics.md](tier3/37-realtime-analytics.md) |
| 38 | Fraud Detection System | [tier3/38-fraud-detection.md](tier3/38-fraud-detection.md) |
| 39 | Digital Wallet | [tier3/39-digital-wallet.md](tier3/39-digital-wallet.md) |
| 40 | Polling / Voting System | [tier3/40-polling-voting.md](tier3/40-polling-voting.md) |
| 41 | Configuration Management System | [tier3/41-config-management.md](tier3/41-config-management.md) |
| 42 | Online Code Editor / Judge (LeetCode) | [tier3/42-online-judge.md](tier3/42-online-judge.md) |
| 43 | Multi-Player Online Game Backend | [tier3/43-multiplayer-game.md](tier3/43-multiplayer-game.md) |
| 44 | Subscription Billing System | [tier3/44-subscription-billing.md](tier3/44-subscription-billing.md) |
| 45 | Geo-Distributed Database | [tier3/45-geo-distributed-db.md](tier3/45-geo-distributed-db.md) |
| 46 | Comment / Threaded Discussion System | [tier3/46-comment-system.md](tier3/46-comment-system.md) |
| 47 | Content Moderation System | [tier3/47-content-moderation.md](tier3/47-content-moderation.md) |
| 48 | Distributed Transaction System | [tier3/48-distributed-transactions.md](tier3/48-distributed-transactions.md) |
| 49 | Feature Flag / A/B Testing System | [tier3/49-feature-flags.md](tier3/49-feature-flags.md) |
| 50 | Social Graph (LinkedIn/Facebook connections) | [tier3/50-social-graph.md](tier3/50-social-graph.md) |
| 51 | Ad Click Aggregation System | [tier3/51-ad-click-aggregation.md](tier3/51-ad-click-aggregation.md) |
| 52 | Event Ticketing System (Ticketmaster) | [tier3/52-event-ticketing.md](tier3/52-event-ticketing.md) |
| 53 | File Synchronization Service | [tier3/53-file-sync.md](tier3/53-file-sync.md) |
| 54 | Typeahead/Autocomplete at Scale | [tier3/54-typeahead-at-scale.md](tier3/54-typeahead-at-scale.md) |
| 55 | Ride-Matching Algorithm (Uber backend deep dive) | [tier3/55-ride-matching.md](tier3/55-ride-matching.md) |
| 56 | Webhook Delivery System | [tier3/56-webhook-delivery.md](tier3/56-webhook-delivery.md) |
| 57 | Short Video Platform (TikTok/Reels) | [tier3/57-short-video-platform.md](tier3/57-short-video-platform.md) |
| 58 | Loyalty / Rewards Points System | [tier3/58-loyalty-rewards.md](tier3/58-loyalty-rewards.md) |
| 59 | Data Pipeline / ETL System | [tier3/59-data-pipeline.md](tier3/59-data-pipeline.md) |
| 60 | Service Discovery System (Consul/Eureka) | [tier3/60-service-discovery.md](tier3/60-service-discovery.md) |

### 🟢 Tier 4 — Specialized / Domain-Specific (61–80)

| # | Topic | File |
|---|-------|------|
| 61 | IoT Data Ingestion Platform | [tier4/61-iot-ingestion.md](tier4/61-iot-ingestion.md) |
| 62 | Healthcare Records System (EHR) | [tier4/62-healthcare-ehr.md](tier4/62-healthcare-ehr.md) |
| 63 | Document Management System | [tier4/63-document-management.md](tier4/63-document-management.md) |
| 64 | Food Delivery Platform (DoorDash/Uber Eats) | [tier4/64-food-delivery.md](tier4/64-food-delivery.md) |
| 65 | Drone/Autonomous Vehicle Fleet System | [tier4/65-fleet-management.md](tier4/65-fleet-management.md) |
| 66 | Contact Center / Call Routing System | [tier4/66-call-routing.md](tier4/66-call-routing.md) |
| 67 | DNS System | [tier4/67-dns-system.md](tier4/67-dns-system.md) |
| 68 | Distributed Tracing System (Jaeger/Zipkin) | [tier4/68-distributed-tracing.md](tier4/68-distributed-tracing.md) |
| 69 | Permission / RBAC System | [tier4/69-rbac-system.md](tier4/69-rbac-system.md) |
| 70 | Authentication System (OAuth/SSO) | [tier4/70-auth-system.md](tier4/70-auth-system.md) |
| 71 | Multi-Tenant SaaS Platform | [tier4/71-multi-tenant-saas.md](tier4/71-multi-tenant-saas.md) |
| 72 | CI/CD Pipeline System | [tier4/72-cicd-pipeline.md](tier4/72-cicd-pipeline.md) |
| 73 | Image Processing / Resizing Service | [tier4/73-image-processing.md](tier4/73-image-processing.md) |
| 74 | Video Transcoding Pipeline | [tier4/74-video-transcoding.md](tier4/74-video-transcoding.md) |
| 75 | Warehouse / Inventory Management System | [tier4/75-inventory-management.md](tier4/75-inventory-management.md) |
| 76 | Coupon / Discount System | [tier4/76-coupon-system.md](tier4/76-coupon-system.md) |
| 77 | Flash Sale / Limited Inventory System | [tier4/77-flash-sale.md](tier4/77-flash-sale.md) |
| 78 | Distributed File System (HDFS-like) | [tier4/78-distributed-filesystem.md](tier4/78-distributed-filesystem.md) |
| 79 | Blockchain / Ledger System | [tier4/79-blockchain-ledger.md](tier4/79-blockchain-ledger.md) |
| 80 | Chatbot / LLM-Powered Service (ChatGPT-like) | [tier4/80-llm-service.md](tier4/80-llm-service.md) |

### ⚪ Tier 5 — Advanced / Niche (81–100)

| # | Topic | File |
|---|-------|------|
| 81 | Consistent Hashing Ring | [tier5/81-consistent-hashing.md](tier5/81-consistent-hashing.md) |
| 82 | Vector Database / Semantic Search | [tier5/82-vector-database.md](tier5/82-vector-database.md) |
| 83 | Time-Series Database | [tier5/83-timeseries-database.md](tier5/83-timeseries-database.md) |
| 84 | Graph Database System | [tier5/84-graph-database.md](tier5/84-graph-database.md) |
| 85 | ML Feature Store | [tier5/85-ml-feature-store.md](tier5/85-ml-feature-store.md) |
| 86 | Model Serving Infrastructure | [tier5/86-model-serving.md](tier5/86-model-serving.md) |
| 87 | Dark Mode / Theme Configuration at Scale | [tier5/87-theme-config-at-scale.md](tier5/87-theme-config-at-scale.md) |
| 88 | GDPR Compliance / Data Deletion Pipeline | [tier5/88-gdpr-pipeline.md](tier5/88-gdpr-pipeline.md) |
| 89 | Multi-Region Failover System | [tier5/89-multi-region-failover.md](tier5/89-multi-region-failover.md) |
| 90 | Audit Logging System | [tier5/90-audit-logging.md](tier5/90-audit-logging.md) |
| 91 | Smart Home Hub | [tier5/91-smart-home-hub.md](tier5/91-smart-home-hub.md) |
| 92 | Supply Chain Tracking System | [tier5/92-supply-chain-tracking.md](tier5/92-supply-chain-tracking.md) |
| 93 | Real-Time Bidding System (Ad Tech) | [tier5/93-real-time-bidding.md](tier5/93-real-time-bidding.md) |
| 94 | API Rate Limiting as a Service | [tier5/94-rate-limiting-service.md](tier5/94-rate-limiting-service.md) |
| 95 | Parking Lot System | [tier5/95-parking-lot.md](tier5/95-parking-lot.md) |
| 96 | Library Management System | [tier5/96-library-management.md](tier5/96-library-management.md) |
| 97 | Hospital Appointment Booking | [tier5/97-hospital-booking.md](tier5/97-hospital-booking.md) |
| 98 | Distributed Saga / Compensation System | [tier5/98-distributed-saga.md](tier5/98-distributed-saga.md) |
| 99 | Zero-Downtime Deployment System | [tier5/99-zero-downtime-deployment.md](tier5/99-zero-downtime-deployment.md) |
| 100 | Chaos Engineering Platform | [tier5/100-chaos-engineering.md](tier5/100-chaos-engineering.md) |

---

## Common Concepts Reference

Before diving into questions, review the shared building blocks all designs draw from:

→ **[resources/common-concepts.md](resources/common-concepts.md)**

Covers: CAP theorem · consistency models · database selection · caching patterns · sharding strategies · load balancing · replication · CDN · message queues · rate limiting algorithms · back-of-envelope math
