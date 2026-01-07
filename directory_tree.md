fraud-ring-detection-mvp/
│
├── .env.example                           # Local dev environment variables
├── .gitignore                             # Git ignore file (includes .env and .streamlit/secrets.toml)
├── requirements.txt                       # Python dependencies
├── README.md                              # Project documentation
├── app.py                                 # Main Streamlit application (Home page)
├── packages.txt                           # System packages for Streamlit Cloud (if needed)
│
├── .streamlit/                            # Streamlit configuration
│   ├── config.toml                        # Streamlit app configuration (theme, etc.)
│   └── secrets.toml.example               # Secrets template for Streamlit Cloud
│
├── analytics/                             # Analytics and ML modules
│   ├── __init__.py
│   ├── risk_scorer.py                     # Risk scoring engine
│   ├── entity_analyzer.py                 # Entity relationship analysis
│   └── similarity_detector.py             # Similarity detection algorithms
│
├── components/                            # Reusable UI components
│   ├── __init__.py
│   ├── graph_visualizer.py                # Network graph visualization
│   ├── risk_explainer.py                  # Risk score explanation UI
│   ├── ring_classifier.py                 # Ring classification display
│   ├── filter_panel.py                    # Data filtering controls
│   └── entity_card.py                     # Entity information cards
│
├── data/                                  # Data layer
│   ├── __init__.py
│   ├── neo4j_driver.py                    # Neo4j database driver (env + secrets aware)
│   │
│   ├── models/                            # Data models
│   │   ├── __init__.py
│   │   ├── fraud_ring.py                  # FraudRing model
│   │   ├── claim.py                       # Claim model
│   │   ├── claimant.py                    # Claimant model
│   │   ├── provider.py                    # Provider model
│   │   ├── attorney.py                    # Attorney model
│   │   └── address.py                     # Address model
│   │
│   └── repositories/                      # Data access layer
│       ├── __init__.py
│       ├── claim_repository.py            # Claims data access
│       ├── ring_repository.py             # Rings data access
│       ├── claimant_repository.py         # Claimants data access
│       ├── provider_repository.py         # Providers data access
│       ├── attorney_repository.py         # Attorneys data access
│       └── address_repository.py          # Addresses data access
│
├── detection/                             # Fraud detection algorithms
│   ├── __init__.py
│   ├── ring_detector.py                   # Main ring detection orchestrator
│   ├── pattern_detectors.py               # Pattern-specific detectors
│   └── feature_engineer.py                # Feature extraction for ML
│
├── pages/                                 # Streamlit pages (MVP)
│   ├── 01_🔥_Hot_Queue.py                 # High-risk claims dashboard
│   ├── 03_🔍_Entity_Profile.py            # Entity deep-dive analysis
│   └── 08_🕸️_Discovered_Rings.py          # Discovered rings review
│
├── services/                              # Business logic services
│   ├── __init__.py
│   ├── case_manager.py                    # Investigation case management
│   └── alert_engine.py                    # Alert generation and management
│
├── utils/                                 # Utility functions
│   ├── __init__.py
│   ├── config.py                          # Config loader (.env + st.secrets)
│   ├── logger.py                          # Logging configuration
│   └── helpers.py                         # Helper functions
│
├── scripts/                               # Utility scripts (for local dev)
│   ├── setup_database.py                  # Database initialization
│   ├── load_sample_data.py                # Load demo data
│   └── test_connection.py                 # Test Neo4j connection
│
├── docs/                                  # Documentation
│   ├── README.md                          # Overview
│   ├── LOCAL_SETUP.md                     # Local development setup
│   ├── STREAMLIT_DEPLOYMENT.md            # Streamlit Cloud deployment guide
│   ├── NEO4J_SETUP.md                     # Neo4j Aura setup guide
│   └── USER_GUIDE.md                      # End-user guide
│
└── assets/                                # Static assets (optional)
    ├── logo.png                           # App logo
    ├── screenshots/                       # App screenshots for docs
    └── demo_data/                         # Sample data files
        └── sample_claims.csv
